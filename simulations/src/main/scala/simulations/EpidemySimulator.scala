package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8

    // to complete: additional parameters of simulation
    val prevalenceRate = 0.01
    val transmissibility = 0.4
    val daysFromInfectedToSick = 6
    val daysFromSickToDead = 8
    val daysFromSickToImmune = 10
    val daysFromImmuneToHealthy = 2
    val daysForNextMove = 5
    val deathRate = 0.25
  }

  import SimConfig._

  val persons: List[Person] = contructPersonList(Nil, population)
  def contructPersonList(persons: List[Person], num: Int): List[Person] = {
    if (num <= 0)
      persons
    else
      contructPersonList((new Person(num - 1)) :: persons, num - 1)
  }
  val prevalenceNum: Int = (prevalenceRate * population).toInt
  while (persons.count(_.infected) != prevalenceNum) {
    persons(randomBelow(population)).setInfected
  }

  class Person (val id: Int) {
    var infected = false
    var sick = false
    var immune = false
    var dead = false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    afterDelay(randomBelow(daysForNextMove) + 1)(move)
    
    //
    // to complete with simulation logic
    //
    
    def setInfected: Unit = {
      infected = true
      afterDelay(daysFromInfectedToSick)(setSick)
    }
    
    def setSick: Unit = {
      sick = true
      if (random < deathRate) {
        afterDelay(daysFromSickToDead)(setDead)
      }
      else {
        afterDelay(daysFromSickToImmune)(setImmune)
      }
    }
    
    def setImmune: Unit = {
      immune = true
      sick = false
      afterDelay(daysFromImmuneToHealthy)(setHealthy)
    }
    
    def setDead: Unit = {
      dead = true
    }
    
    def setHealthy: Unit = {
      immune = false
      infected = false
    }  
    
    def move: Unit = {
      if (!dead) {
        val moveDirection: Int = randomBelow(4)
        if (moveDirection == 0) {
          row += 1
          if (row >= roomRows)
             row = 0
        }  
        else if (moveDirection == 1) {
          row -= 1
          if (row < 0)
            row = roomRows - 1
        }
        else if (moveDirection == 2) {
          col += 1
          if (col >= roomColumns)
            col = 0
        }
        else {
          col -= 1
          if (col < 0) {
            col = roomColumns - 1
          }
        }
        if (persons.filter(p => (p.row == row && p.col == col)).exists(p => p.infected)) {
          if (random < transmissibility) {
            if (!infected && !sick && !immune && !dead)
              setInfected
          }
        }
        afterDelay(randomBelow(daysForNextMove) + 1)(move)
      }
    }
    
    
  }
}
