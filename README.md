
# FunHouse

Getting a feel for clickhouse and column-oriented data via the low-level [ch-go](https://github.com/ClickHouse/ch-go)!

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

Both work, but at the moment, I'd go to prod with "generable" and the scant remains of the readme will focus there.

## What are these Msg's Anyway

I started with the `ch-go` example [examples/insert/main.go](https://github.com/ClickHouse/ch-go/blob/main/examples/insert/main.go).






Coming soon: more blather!!
