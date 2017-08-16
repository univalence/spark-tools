


```scala

case class Hello(name: String, greet: Boolean)

object Hello {
  @autoBuildResult
  def build:Result[Hello] = ???
}

```scala
Error:(__, __) signature not matching to build Hello, use : 
 @autoBuildResult
 def build(name : Result[String],
           greet : Result[Boolean]):Result[Hello] = MacroMarker.generated_applicative
      @autoBuildResult
```


