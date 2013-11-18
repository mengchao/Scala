package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5
  
  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run
    
    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "and 3")
  }
 
  //
  // to complete with tests for orGate, demux, ...
  //
  test("orGate") {
    val in1, in2, out = new Wire
    orGate(in1, in2, out)
    testOrLogic(in1, in2, out)
  }

  test("orGate2") {
    val in1, in2, out = new Wire
    orGate2(in1, in2, out)
    testOrLogic(in1, in2, out)
  } 
  
  def testOrLogic(in1: Wire, in2: Wire, out: Wire): Unit = {
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "or 1")

    in1.setSignal(true)
    run
    
    assert(out.getSignal === true, "or 2")

    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "or 3")
  }
  

  test("demux example") {
    val in = new Wire
    val c = (new Wire) :: (new Wire) :: Nil
    val out = (new Wire) :: (new Wire) :: (new Wire) :: (new Wire) :: Nil
    demux(in, c, out)
    
    in.setSignal(true)
    run
    assert(out(3).getSignal === true, "demux 1")
    

    
  }
}
