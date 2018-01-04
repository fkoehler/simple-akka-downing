package com.ajjpj.simpleakkadowning.util

import akka.remote.testconductor.RoleName

abstract class AbstractDowningSpec(config: SimpleDowningConfig, survivors: Int*) extends MultiNodeClusterSpec(config) {
  val side1 = survivors.map(s => RoleName(s"$s")).toVector //  Vector (node1, node2, node3)
  // role node0 is the conductor which is not participating in the cluster
  val side2 = roles.tail.filterNot (side1.contains) //Vector (node4, node5)

  "A cluster of five nodes" should {
    "reach initial convergence" in {
      muteLog()
//      muteMarkingAsUnreachable()
//      muteMarkingAsReachable()

      awaitClusterUp(side1 ++ side2 :_*)

      if ((side1 ++ side2).contains(myself)) {
        log.info("------ " + myself + " sees the cluster state as: \n" + cluster.state.members.mkString("\n"))
      }
      enterBarrier("after-1")
    }

    // TODO: This tests actually tests nothing because it does not wait for the failure detector to actually do the partition
    // i takes at least roughly failure-detector.heartbeat-interval i assume to change the cluster state
    // we should probably check state via conductr here as well

//    "mark nodes as unreachable between partitions, and heal the partition" in {
//      enterBarrier ("before-split")
//       mark nodes across the partition as mutually unreachable, and wait until that is reflected in all nodes' local cluster state
//      createNetworkPartition(side1, side2)
//      enterBarrier ("after-split")
//
//       mark nodes across the partition as mutually unreachable, and wait until that is reflected in all nodes' local cluster state
//      healNetworkPartition()
//      enterBarrier ("after-network-heal")
//
//      runOn (config.conductor) {
//        for (r <- side1 ++ side2) {
//          upNodesFor (r) shouldBe (side1 ++ side2).toSet
//          unreachableNodesFor (r) shouldBe empty
//        }
//      }
//
//      enterBarrier ("after-cluster-heal")
//    }

    "detect a network partition and shut down one partition after a timeout" in {
      enterBarrier("before-durable-partition")
      log.info("------------ PASSED before-durable-partition")

//       mark nodes across the partition as mutually unreachable, and wait until that is reflected in all nodes' local cluster state
      createNetworkPartition(side1, side2)
      enterBarrier("durable-partition")
      log.info("------------ PASSED durable-partition")

      Thread.sleep(2000)

      if ((side1 ++ side2).contains(myself)) {
        log.info("------ " + myself + " sees the cluster state INTERNALLY as: \n" + cluster.state.members.mkString("\n") +
          "\n unreachable: " + cluster.state.unreachable.flatMap(_.address.port).map(portToNode).mkString(", "))
      }
      runOn (config.conductor) {
        log.info("CONDUCTR sees this cluster state via HTTP requests against the nodes: ")
        for (r <- side1 ++ side2) {
          log.info(s"UP nodes for $r are: " + upNodesFor(r).toList.sortBy(_.name).map(s => s"${s.name} ${s.port.get}").sorted.mkString(", "))
          log.info(s"DOWN nodes for $r are: " + unreachableNodesFor(r).toList.sortBy(_.name).map(s => s"${s.name} ${s.port.get}").mkString(", "))
          log.info("")
        }
      }

//
//       five second timeout until our downing strategy kicks in - plus some additional delay to be on the safe side
      Thread.sleep(15000)

      if ((side1 ++ side2).contains(myself)) {
        log.info("------ " + myself + " sees the cluster state INTERNALLY as: \n" + cluster.state.members.mkString("\n") +
          "\n unreachable: " + cluster.state.unreachable.flatMap(_.address.port).map(portToNode).mkString(", "))
      }
      runOn (config.conductor) {
        log.info("CONDUCTR sees this cluster state via HTTP requests against the nodes: ")
        for (r <- side1 ++ side2) {
          log.info(s"UP nodes for $r are: " + upNodesFor(r).toList.sortBy(_.name).map(s => s"${s.name} ${s.port.get}").sorted.mkString(", "))
          log.info(s"DOWN nodes for $r are: " + unreachableNodesFor(r).toList.sortBy(_.name).map(s => s"${s.name} ${s.port.get}").mkString(", "))
          log.info("")
        }
      }


      // VERIFY
      runOn (config.conductor) {
        for (r <- side1) upNodesFor(r) shouldBe side1.toSet
        for (r <- side2) upNodesFor(r) shouldBe empty
      }

//    some additional time to ensure cluster node JVMs stay alive during the previous checks
      Thread.sleep(10000)
    }
  }
}
