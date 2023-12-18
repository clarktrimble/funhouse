
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
  err = mt.Client.Do(ctx, ch.Query{
    Body:  input.Into(mt.Table),
    Input: input,
    OnInput: func(ctx context.Context) error {

      input.Reset()
      if idx > mcs.Length {
        return io.EOF
      }

      end := min(idx+chunkSize, mcs.Length)

      mt.Ts.AppendArr(mcs.Timestamps[idx:end])
      mt.SeverityTxt.AppendArr(mcs.SeverityTxts[idx:end])
      mt.SeverityNum.AppendArr(mcs.SeverityNums[idx:end])
      mt.Body.AppendArr(mcs.Bodies[idx:end])
      mt.Name.AppendArr(mcs.Names[idx:end])
      mt.Arr.AppendArr(mcs.Tagses[idx:end])

      idx += chunkSize
      return nil
    },
  })
```

Clearly this code is irrevocably bound to the message type, but has a virtuous simplicity and of course could be generated if warranted.

MsgTable fields (i.e.: `mt.Ts`) are the same as used to build `input` with:

## Get!!

Reading the msgs back out is currently left as an exercise to the reader in `ch-go`.

Shockingly, it's somewhat similar to the above:

```go
  err = mt.Client.Do(ctx, ch.Query{
    Body:   fmt.Sprintf("select * from %s", mt.Table),
    Result: results,
    OnResult: func(ctx context.Context, block proto.Block) error {

      mcs.Length += block.Rows
      for _, col := range results {
        switch col.Name {
        case "ts":
          flt.Append(&mcs.Timestamps, mt.Ts)
        case "severity_text":
          flt.Append(&mcs.SeverityTxts, mt.SeverityTxt)
        case "severity_number":
          flt.Append(&mcs.SeverityNums, mt.SeverityNum)
        case "body":
          flt.Append(&mcs.Bodies, mt.Body)
        case "name":
          flt.Append(&mcs.Names, mt.Name)
        case "arr":
          flt.Append(&mcs.Tagses, mt.Arr)
        }
        col.Data.Reset()
      }

      return mcs.CheckLen()
    },
  })
```

The tricky part for me was to (mostly) ignore `block` in the callback and pull the results from, ah, `results`, in which the columns are packed up in a similar manner to `input` above.

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
type MsgTable struct {
  ...
  Ts          *proto.ColDateTime64
  SeverityTxt *proto.ColEnum
  SeverityNum *proto.ColUInt8
  Body        *proto.ColStr
  Name        *proto.ColStr
  Arr         *proto.ColArr[string]
}
```

A helper, um, helps transform this to `input`:

```go
func Input(cnr ColNamer) (input proto.Input, err error) {

  cols, names := cnr.ColNames()
  if len(cols) != len(names) {
    err = fmt.Errorf("unequal number of columns and names")
    return
  }

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

With a quite similar helper for `results`.

_A-and_ we can still refer to the concrete types in `OnInput` and `OnResult` without unsightly assertions via MsgTable instance!

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
 - somehow extract "chunking" logics from type-specific codeses
 - explore the world of indexing in ch

