package sparkscalademos.transformations

import org.apache.spark.SparkContext
import sparkscalademos._

/*
 * Copyright (C) 2016 Iryna Kharaborkina.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
object PairTransformationDemo extends App {
  val sc = new SparkContext(configure(getClass.getSimpleName))
  val pairs = (1 to 6) zip (11 to 16)
  val pairRDD = sc.parallelize(pairs) // ((1,11), (2,12), (3,13), (4,14), (5,15), (6,16))
  printRdd("Initial", pairRDD)

  val valueMapped = pairRDD.mapValues(_ * 2)
  printRdd("Map values example", valueMapped) // ((1,22), (2,24), (3,26), (4,28), (5,30), (6,32))

  val valueFlatMapped = pairRDD.flatMapValues(v => v to v + 1)
  printRdd("Flat map values example", valueFlatMapped) //((1,11), (1,12), (2,12), (2,13), (3,13), (3,14), ... (6,17))

  val sorted = pairRDD.sortByKey(ascending = false)
  printRdd("Sort descending example", sorted) // ((6,16), (5,15), (4,14), (3,13), (2,12), (1,11))

  val keys = pairRDD.keys
  printRdd("Keys only example", keys) // (1, 2, 3, 4, 5, 6)

  val values = pairRDD.values
  printRdd("Values only example", values) // (11, 12, 13, 14, 15, 16)


  val pairsWithDuplicates = (1 to 6) map (_ / 3) zip (11 to 16)
  val duplicatesRdd = sc.parallelize(pairsWithDuplicates)
  printRdd("pairs with duplicated keys", duplicatesRdd) // ((0,11), (0,12), (1,13), (1,14), (1,15), (2,16))

  val reduced = duplicatesRdd.reduceByKey(_ + _)
  printRdd("Reduce by key example", reduced) // ((0,23), (1,42), (2,16))

  val grouped = duplicatesRdd.groupByKey()
  printRdd("Group by key example", grouped) // ((0,(11, 12)), (1,(13, 14, 15)), (2,(16)))

  val sumToTuple = {
    case ((sum: Int, total: Int), b: Int) => sum + b -> (total + 1)
  }

  val sumTwoTuple = {
    case ((firstSum: Int, firstTotal: Int), (secondSum: Int, secondTotal: Int)) =>
      (firstSum + secondSum, firstTotal + secondTotal)
  }

  val aggregated = duplicatesRdd.aggregateByKey((0, 0))(sumToTuple, sumTwoTuple)
  printRdd("Aggregate by key example", aggregated) // (0,(23,2)), (1,(42,3)), (2,(16,1))

  sc.stop()
}
