
# FunHouse

Getting a feel for ClickHouse and column-oriented data via the low-level [ch-go](https://github.com/ClickHouse/ch-go)!

## Column-Oriented You Say?

Indeed! :)
I'm thinking this boils down to reading and writing data in blocks rather than rows.
In our beloved Golang, such an idea can be represented thusly:

```go
type Msg struct {
  Timestamp time.Time
  Severity  Severity
  Name      string
  Body      string
  Tags      []string
}

type MsgCols struct {
  Length       int
  Timestamps   []time.Time `col:"ts"`
  SeverityTxts []string    `col:"severity_text"`
  SeverityNums []uint8     `col:"severity_number"`
  Names        []string    `col:"name"`
  Bodies       []string    `col:"body"`
  Tagses       [][]string  `col:"arr"`
}
```

Where `Msg` is row-oriented and `MsgCols` is block or column oriented.
Of course, there's much, much more to the nigh-on obsessive performance at ClickHouse, and I recommend [this page](https://clickhouse.com/docs/en/optimize/sparse-primary-indexes) for a good read.
The explanation about indexes necessarily gives you a good feel for it's blocky nature.

## A Tale of Two Implementations

Or three!
Still finding my way with the voluminous, sparsely documented `ch-go`.
Kind of a fun challenge and certainly educational :)

`examples/msgdemo/main.go` is an entrypoint to the currently favored approach, from which I would develop had I the good fortune to be slinging trillions of records at sprawling ClickHouse cluster sometime soon.

## What are these Msg's Anyway

I started with the `ch-go` example [examples/insert/main.go](https://github.com/ClickHouse/ch-go/blob/main/examples/insert/main.go) and `Msg` got its start there.

Here's how my take on insert turned out:

```go
func (fh *Fh) PutInput(ctx context.Context, chunkSize int, tbr Tabler) (err error) {
  var idx int
  total := tbr.Total()

  input, err := input(tbr)
  if err != nil {
    return
  }

  err = fh.Client.Do(ctx, ch.Query{
    Body:  input.Into(tbr.TableName()),
    Input: input,
    OnInput: func(ctx context.Context) error {
      input.Reset()
      if idx > total {
        return io.EOF
      }
      end := min(idx+chunkSize, total)

      tbr.AppendTo(idx, end)

      idx += chunkSize
      return nil
    },
  })
  return
}
```

```go
func (mt *MsgTable) AppendTo(idx, end int) {
  mt.Cols.Ts.AppendArr(mt.Data.Timestamps[idx:end])
  mt.Cols.SeverityTxt.AppendArr(mt.Data.SeverityTxts[idx:end])
  // ... the rest of the cols
}
```

`MsgTable` implements a `Tabler` interface, letting it focus on messages particulars, while the `funlite` package provides reusable ClickHouse'isms.

Concrete column types in `MsgTable` hold it all together. Funlite's `input` helper references them when creating `input` for use in `Do`.  You'll see something quite similar in `PutInput` just below.

## Get!!

Reading the msgs back out is currently left as an exercise to the reader in `ch-go`.

Shockingly, it's somewhat similar to `PutInput` above:

```go
func (fh *Fh) GetResults(ctx context.Context, tbr Tabler) (err error) {
  results, err := results(tbr)
  if err != nil {
    return
  }

  err = fh.Client.Do(ctx, ch.Query{
    Body:   fmt.Sprintf("select * from %s", tbr.TableName()),
    Result: results,
    OnResult: func(ctx context.Context, block proto.Block) error {

      return tbr.AppendFrom(block.Rows, results)
    },
  })
  return
}
```

```go
func (mt *MsgTable) AppendFrom(count int, results proto.Results) (err error) {

  mt.Data.Length += count
  for _, col := range results {
    switch col.Name {
    case "ts":
      flt.Append(&mt.Data.Timestamps, mt.Cols.Ts)
    case "severity_text":
      flt.Append(&mt.Data.SeverityTxts, mt.Cols.SeverityTxt)
    // ... the rest of the cols
    }
    col.Data.Reset()
  }

  return mt.Data.CheckLen()
}
```

The tricky part for me was to (mostly) ignore `block` in the callback and pull the results from `results`.

Not too horrid thanks to a generic `Append`:

```go
func Append[T any](slice *[]T, rr proto.ColumnOf[T]) {

  for i := 0; i < rr.Rows(); i++ {
    *slice = append(*slice, rr.Row(i))
  }
}
```

I'm not sure if `Reset`ing the columns is needed when reading, but doesn't seem to hurt.

## Enclosing the Cols

Above we see "put" sending data via an enclosed `input` and "get" reading via an enclosed `results`, both of which refer to concrete types established in MsgTable:

```go
type Cols struct {
  Ts          *proto.ColDateTime64
  SeverityTxt *proto.ColEnum
  SeverityNum *proto.ColUInt8
  Body        *proto.ColStr
  Name        *proto.ColStr
  Arr         *proto.ColArr[string]
}
type MsgTable struct {
  // ...
  Cols   Cols
}
```

A helper transforms this to `input`:

```go
func input(tbr Tabler) (input proto.Input, err error) {
  cols, names := tbr.ColNames()

  input = proto.Input{}
  for i, name := range names {
    input = append(input, proto.InputColumn{
      Name: name,
      Data: cols[i],
    })
  }

  return
}
```

With a similar helper for `results`.

_A-and_ we can refer to the concrete types in `OnInput` and `OnResult` without unsightly assertions!

## The other implementation(s)

After getting basic write/read working, I was carried away factoring a particular table/struct from re-usable "funhouse" code.
[reflecting/msgtable](https://github.com/clarktrimble/funhouse/blob/main/examples/reflecting/msgtable/msgtable.go) isolates all the "msg" particulars.  In support of this laudable goal, we have:
 - `funhouse` providing general get/put column methods
 - `table` providing a little table abstraction
 - `colspec` providing a "col" struct tag and column-oriented reflection.
Yay!

I'm not quite ready to trash it yet.

## Up Next

 - find a better place to park other imp's
 - look at performance
 - explore the world of indexing in ch

