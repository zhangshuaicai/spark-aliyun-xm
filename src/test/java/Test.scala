import scala.math.BigDecimal.RoundingMode

object Test {
    def main(args: Array[String]): Unit = {
        println(BigDecimal("98.7511").setScale(1, RoundingMode.HALF_UP).doubleValue())
    }
}
