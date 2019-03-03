package spaeml

import scala.collection.mutable

/**
  * Keeps track of the state each SNP/SNP_combination is in
  *
  * When going through the model building process, each SNP/SNP_combination can be in one of three states:
  *   not_added: has not yet been included in the model, still under consideration
  *   added_prev: was added in a previous iteration, will be included in the model in addition to the current SNP being
  *                 considered
  *   skipped: was removed in the backward step of the last iteration; after being removed, SNPs are not considered for
  *              one iteration
  *
  * Furthermore, during each step, we need to broadcast a table with the values for all of the SNPs in the added_prev
  *   category. Because we do not want to look these up at the beginning of each iteration, it is useful to keep track
  *   of them here, alongside the state of each SNP. We can also ensure that if a SNP is removed from the added_prev
  *   category, its values are removed from the added_prev table as well
  */
class StepCollections(not_added: mutable.HashSet[String],
                      added_prev: mutable.HashSet[String] = mutable.HashSet(),
                      skipped: mutable.HashSet[String] = mutable.HashSet()) extends Serializable {

  def copy(): StepCollections = {
    new StepCollections(not_added, added_prev, skipped)
  }

  /*
      Direction of flow of SNP names in StepCollections

             +>-----> added_prev ---->-+
            /                            \
        not_added                       skipped
           ^                             /
            \   (after one iteration)   /
             +<-----<-----<-----<-----<+
 */

  val addedPrevValues: mutable.HashMap[String, breeze.linalg.DenseVector[Double]] = new mutable.HashMap()

  def getNotAdded: mutable.HashSet[String] = not_added
  def getAddedPrev: mutable.HashSet[String] = added_prev
  def getSkipped: mutable.HashSet[String] = skipped

  private def moveFrom(from: mutable.HashSet[String], to: mutable.HashSet[String], snpName: String): Unit = {
    from.remove(snpName)
    to.add(snpName)
  }

  def moveFromNotAdded2AddedPrev(snpName: String, snpValues: breeze.linalg.DenseVector[Double]): Unit = {
    moveFrom(not_added, added_prev, snpName)
    println("Moved " + snpName + " from NOT ADDED to ADDED PREVIOUSLY")
    addedPrevValues.put(snpName, snpValues)
  }

  def moveFromAddedPrev2Skipped(snpName: String): Unit = {
    moveFrom(added_prev, skipped, snpName)
    println("Moved " + snpName + " from ADDED PREVIOUSLY to SKIPPED")
    addedPrevValues.remove(snpName)
  }

  def moveFromSkipped2NotAdded(snpName: String): Unit = {
    println("Moved " + snpName + " from SKIPPED to NOT ADDED")
    moveFrom(skipped, not_added, snpName)
  }

}