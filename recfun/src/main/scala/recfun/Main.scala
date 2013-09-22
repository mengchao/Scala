package recfun
import common._

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = 
  {
    if (c < 0 || r < 0 || c > r)
    {
      throw new IllegalArgumentException()
    }
    else if (c == 0 || c == r)
    {
      1
    }
    else
    {
      pascal(c-1, r-1) + pascal(c, r-1)
    }
  }

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = 
  {
    def isListBalanced(numOfOpenParentheses: Int, chars : List[Char]) : Boolean =
    {
      if (chars.isEmpty)
      {
        numOfOpenParentheses == 0
      }
      else if (numOfOpenParentheses < 0)
      {
        false
      }
      else 
      {
        val headElem = chars.head
        val remainingOpenParentheses = 
          if (headElem == ')')
            numOfOpenParentheses - 1
          else if (headElem == '(')
            numOfOpenParentheses + 1
          else
            numOfOpenParentheses
         isListBalanced(remainingOpenParentheses, chars.tail)
      }
    }
    isListBalanced(0, chars)
  }

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = 
  {
    if (money < 0)
    {
      0
    }
    else if (money == 0)
    {
      1
    }
    else if (coins.isEmpty)
    {
      0
    }
    else
    {
      countChange(money - coins.head, coins) + countChange(money, coins.tail)
    }
  }
}
