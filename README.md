# πFlow
a full big data flow system involving online data collection, real time computation, data output

```
        ______ _
       |  ____| |
 ______| |__  | | _____      __
/_ _  _|  __| | |/ _ \ \ /\ / /
 || ||_| |    | | (_) \ V  V /
 |/  \/|_|    |_|\___/ \_/\_/
	
```

#piflow-shell

```
//sequence
SeqAsSource(1, 2, 3, 4) > DoMap[Int, Int](_ + 10) > ConsoleSink()

//merge
val line1 = SeqAsSource(1, 2, 3, 4) > DoMap[Int, Int](_ + 10);
val line2 = SeqAsSource("a", "b", "c", "d") > DoMap[String, String](_.toUpperCase());
val line3 = Seq(line1 > "_1:_1", line2 > "_1:_2") > DoZip[Int, String]() > ConsoleSink();

//fork
val line = SeqAsSource(1, 2, 3, 4) > DoFork[Int](_ % 2 == 0, _ % 2 == 1);
line > "_1:_1" > DoMap[Int, Int](_ + 10) > ConsoleSink();
line > "_2:_1" > DoMap[Int, Int](_ * 10) > ConsoleSink();
```
