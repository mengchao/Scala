package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == Math.min(a, b)
  }
  
  property("empty1") = forAll { a: Int =>
    val h = insert(a, empty)
    deleteMin(h) == empty
  }
  
  property("empty2") = forAll { a: Int =>
    val h = insert(a, empty)
    isEmpty(deleteMin(h))
  }

  property("meld") = forAll { (a: H, b: H) => !(isEmpty(a) || isEmpty(b)) ==>
    (findMin(meld(a, b)) == findMin(a)) || (findMin(meld(a, b)) == findMin(b))
  }

  property("insert, findMin and deleteMin") = forAll { l: List[Int] =>
    def constructHeap(h: H, l: List[Int]): H = l match {
      case Nil => h
      case x :: xs => constructHeap(insert(x, h), xs)
    }
    def testHeapOrder(h: H, l: List[Int]): Boolean = l match {
      case Nil => isEmpty(h)
      case x :: xs => !isEmpty(h) && findMin(h) == x && testHeapOrder(deleteMin(h), xs)
    }
    testHeapOrder(constructHeap(empty, l), l.sorted)
  }
  
  lazy val genHeap: Gen[H] = for {
    k <- arbitrary[Int]
    h <- oneOf(value(empty), genHeap)
  } yield insert(k, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
