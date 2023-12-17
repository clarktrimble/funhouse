
# FunHouse

Getting a feel for ClickHouse and column-oriented data via the low-level [ch-go](https://github.com/ClickHouse/ch-go)!

## Column-Oriented You Say?

Indeed! :)
I'm thinking this boils down to reading and writing data in blocks rather than rows.
In our beloved Golang, such an idea can be represented thusly:

```go
type Severity struct {
	Txt string
	Num uint8
}
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

After getting basic write/read working, I was carried away factoring a particular table/struct from re-usable "funhouse" code.
[reflecting/msgtable](https://github.com/clarktrimble/funhouse/blob/main/examples/reflecting/msgtable/msgtable.go) isolates all the "msg" particulars.  In support of this laudable goal, we have:
 - `funhouse` providing general get/put column methods
 - `table` providing a little table abstraction
 - `colspec` providing a "col" struct tag and column-oriented reflection.
Yay!

However, I soon began to miss the simplicity of the basic write/read code as seen in [generable/msg](https://github.com/clarktrimble/funhouse/blob/main/examples/generable/msg/msg.go).  Such code could be copypasta or generated if warranted and I like it's lack of guile.

Both work, but at the moment, I'd go to prod with "generable" and the scant remains of the readme will focus on it.

## What are these Msg's Anyway

I started with the `ch-go` example [examples/insert/main.go](https://github.com/ClickHouse/ch-go/blob/main/examples/insert/main.go) and `Msg` got its start there.

Here's how the insert turned out:

```go
err = client.Do(ctx, ch.Query{
	Body:  input.Into(tableName),
	Input: input,
	OnInput: func(ctx context.Context) error {

		input.Reset()
		if idx > mcs.Length {
			return io.EOF
		}

		end := min(idx+chunkSize, mcs.Length)

		dataCols["ts"].(*proto.ColDateTime64).AppendArr(mcs.Timestamps[idx:end])
		dataCols["severity_text"].(*proto.ColEnum).AppendArr(mcs.SeverityTxts[idx:end])
		dataCols["severity_number"].(*proto.ColUInt8).AppendArr(mcs.SeverityNums[idx:end])
		dataCols["name"].(*proto.ColStr).AppendArr(mcs.Names[idx:end])
		dataCols["body"].(*proto.ColStr).AppendArr(mcs.Bodies[idx:end])
		dataCols["arr"].(*proto.ColArr[string]).AppendArr(mcs.Tagses[idx:end])

		idx += chunkSize
		return nil
	},
})
```

`input` is built from `dataCols` using the following:

```go
func Input(names []string, byName map[string]proto.Column) (input proto.Input) {

	input = proto.Input{}

	for _, name := range names {
		input = append(input, proto.InputColumn{
			Name: name,
			Data: byName[name],
		})
	}

	return
}
```

The `AppendArr` lines are a little awkward but getting the job done in a T34 kind of way..

I believe, but have not tested, that returning `EOF` from the callback without having returned nil at least once, strands the remaining data.

## G-g-get!!

Reading the msgs back out is currently left as an exercise to the reader in `ch-go`.
Here's how it went here:

```go
  err = client.Do(ctx, ch.Query{
    Body:   fmt.Sprintf(qSpec, tableName),
    Result: results,
    OnResult: func(ctx context.Context, block proto.Block) error {

      mcs.Length += block.Rows
      for _, col := range results {
        switch col.Name {
        case "ts":
          fl.Append(&mcs.Timestamps, col.Data.(*proto.ColDateTime64))
        case "severity_text":
          fl.Append(&mcs.SeverityTxts, col.Data.(*proto.ColEnum))
        case "severity_number":
          fl.Append(&mcs.SeverityNums, col.Data.(*proto.ColUInt8))
        case "name":
          fl.Append(&mcs.Names, col.Data.(*proto.ColStr))
        case "body":
          fl.Append(&mcs.Bodies, col.Data.(*proto.ColStr))
        case "arr":
          fl.Append(&mcs.Tagses, col.Data.(*proto.ColArr[string]))
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

Oh, and I'm not sure if `Reset`ing the columns is needed, but doesn't seem to hurt.

## Up Next

 - look at performance
 - can AppendArr, Row, Rows commonalities be exploited for simpler code?
 - explore the world of indexing in ch

