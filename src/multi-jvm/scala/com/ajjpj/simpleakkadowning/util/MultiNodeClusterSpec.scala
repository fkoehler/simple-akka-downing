package com.ajjpj.simpleakkadowning.util

import akka.actor.{ActorSystem, Address, Props}
import akka.cluster.Member.addressOrdering
import akka.cluster._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.remote.DefaultFailureDetectorRegistry
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.stream.ActorMaterializer
import akka.testkit.TestEvent.Mute
import akka.testkit.{EventFilter, ImplicitSender}

import scala.collection.immutable
import scala.concurrent.duration._


/**
  * Based on test code of Akka cluster itself
  */
abstract class MultiNodeClusterSpec(config: SimpleDowningConfig) extends MultiNodeSpec(config) with STMultiNodeSpec with ImplicitSender {
  def initialParticipants = roles.size

  def cluster: Cluster = Cluster(system)

  val cachedAddresses = {
    roles.map(r => r -> node(r).address).toMap
  }

  implicit def address(role: RoleName): Address = cachedAddresses(role)

  implicit val clusterOrdering: Ordering[RoleName] = new Ordering[RoleName] {
    import Member.addressOrdering
    def compare(x: RoleName, y: RoleName) = addressOrdering.compare(address(x), address(y))
  }

  def assertLeader(nodesInCluster: RoleName*): Unit =
    if (nodesInCluster.contains(myself)) assertLeaderIn(nodesInCluster.to[immutable.Seq])

  def assertLeaderIn(nodesInCluster: immutable.Seq[RoleName]): Unit =
    if (nodesInCluster.contains(myself)) {
      nodesInCluster.length should not be (0)
      val expectedLeader = roleOfLeader(nodesInCluster)
      val leader = cluster.state.leader
      val isLeader = leader == Some(cluster.selfAddress)
      assert(
        isLeader == isNode(expectedLeader),
        "expectedLeader [%s], got leader [%s], members [%s]".format(expectedLeader, leader, cluster.state.members))
      cluster.selfMember.status should (be(MemberStatus.Up) or be(MemberStatus.Leaving))
    }

  def roleOfLeader(nodesInCluster: immutable.Seq[RoleName] = roles): RoleName = {
    nodesInCluster.length should not be (0)
    nodesInCluster.sorted.head
  }


  def awaitClusterUp(roles: RoleName*): Unit = {
    runOn(roles.head) {
      // make sure that the node-to-join is started before other join
      startClusterNode()
    }
    enterBarrier(roles.head.name + "-started")
    if (roles.tail.contains(myself)) {
      cluster.join(roles.head)
    }
    if (roles.contains(myself)) {
      system.actorOf(Props(new ClusterHttpInspector(httpPort(myself))), "http-server")
      awaitMembersUp(numberOfMembers = roles.length)
    }
    println(" entering BARRIER ------------- " + roles.map(_.name).mkString("-") + "-joined")
    enterBarrier(roles.map(_.name).mkString("-") + "-joined")
  }

  private def httpPort (node: RoleName) = {
    val nodeNo = roles.indexOf(node)
    require(nodeNo > 0)
    8080 + nodeNo
  }

  private def awaitMembersUp(numberOfMembers:          Int,
                             canNotBePartOfMemberRing: Set[Address]   = Set.empty,
                             timeout:                  FiniteDuration = 25.seconds): Unit = {
    within(timeout) {
      if (canNotBePartOfMemberRing.nonEmpty) // don't run this on an empty set
        awaitAssert(canNotBePartOfMemberRing foreach (a ⇒ cluster.state.members.map(_.address) should not contain (a)))
      awaitAssert(cluster.state.members.size should ===(numberOfMembers))
      awaitAssert(cluster.state.members.map(_.status) should ===(Set(MemberStatus.Up)))
      // clusterView.leader is updated by LeaderChanged, await that to be updated also
      val expectedLeader = cluster.state.members.collectFirst {
        case m if m.dataCenter == cluster.settings.SelfDataCenter ⇒ m.address
      }
      awaitAssert(cluster.state.leader should ===(expectedLeader))
    }
  }

  private def startClusterNode(): Unit = {
    if (cluster.state.members.isEmpty) {
      cluster join myself
      awaitAssert(cluster.state.members.map(_.address) should contain(address(myself)))
    } else
      cluster.selfMember
  }

  def createNetworkPartition(side1: Seq[RoleName], side2: Seq[RoleName]): Unit = {
    runOn(side1 :_*) {
      for (r <- side2) markNodeAsUnavailable(r)
      awaitAssert(cluster.state.unreachable.size == side2.size)
    }
    runOn(side2 :_*) {
      for (r <- side1) markNodeAsUnavailable(r)
      awaitAssert(cluster.state.unreachable.size == side1.size)
    }
  }

  def healNetworkPartition(): Unit = {
    runOn(roles.tail :_*) {
      for (r <- roles.tail) markNodeAsAvailable(r)
      awaitAssert(cluster.state.unreachable.isEmpty)
    }
  }

  /**
    * Marks a node as available in the failure detector
    */
  private def markNodeAsAvailable(address: Address): Unit =
    failureDetectorPuppet(address) foreach (_.markNodeAsAvailable())

  /**
    * Marks a node as unavailable in the failure detector
    */
  private def markNodeAsUnavailable(address: Address): Unit = {
    // before marking it as unavailable there should be at least one heartbeat
    // to create the FailureDetectorPuppet in the FailureDetectorRegistry
    cluster.failureDetector.heartbeat(address)
    failureDetectorPuppet(address) foreach (_.markNodeAsUnavailable())
  }

  private def failureDetectorPuppet(address: Address): Option[FailureDetectorPuppet] = {
    cluster.failureDetector match {
      case reg: DefaultFailureDetectorRegistry[Address] =>
        // do reflection magic because 'failureDetector' is only visible from within "akka" and sub packages
        val failureDetectorMethod = reg.getClass.getMethods.find(_.getName == "failureDetector").get
        failureDetectorMethod.invoke(reg, address) match {
          case Some(p: FailureDetectorPuppet) => Some(p)
          case _ => None
        }
//          reg.failureDetector(address) collect { case p: FailureDetectorPuppet ⇒ p }
      case _ => None
    }
  }


  def muteLog(sys: ActorSystem = system): Unit = {
    if (!sys.log.isDebugEnabled) {
      Seq(
        ".*Cluster Node.* - registered cluster JMX MBean.*",
        ".*Cluster Node.* Welcome.*",
        ".*Cluster Node.* is JOINING.*",
        ".*Cluster Node.* Leader can .*",
        ".*Cluster Node.* Leader is moving node.*",
        ".*Cluster Node.* - is starting up.*",
        ".*Shutting down cluster Node.*",
        ".*Cluster node successfully shut down.*",
        ".*Ignoring received gossip .*",
        ".*Using a dedicated scheduler for cluster.*") foreach { s ⇒
        sys.eventStream.publish(Mute(EventFilter.info(pattern = s)))
      }

      muteDeadLetters(classOf[AnyRef])(sys)
    }
  }

  def muteMarkingAsUnreachable(sys: ActorSystem = system): Unit =
    if (!sys.log.isDebugEnabled)
      sys.eventStream.publish(Mute(EventFilter.error(pattern = ".*Marking.* as UNREACHABLE.*")))

  def muteMarkingAsReachable(sys: ActorSystem = system): Unit =
    if (!sys.log.isDebugEnabled)
      sys.eventStream.publish(Mute(EventFilter.info(pattern = ".*Marking.* as REACHABLE.*")))


  def upNodesFor(node: RoleName): Set[RoleName] = httpGetNodes(node, "/cluster-members/up")

  def unreachableNodesFor(node: RoleName): Set[RoleName] = httpGetNodes(node, "/cluster-members/unreachable")

  private def httpGetNodes(node: RoleName, path: String): Set[RoleName] = {
    try {

      implicit val mat = ActorMaterializer()

      val uri = Uri (s"http://localhost:${httpPort (node)}$path")
      val response = Http (system).singleRequest (HttpRequest (uri = uri)).await
      val strict = response.entity.toStrict (10.seconds).await
      strict.data.decodeString ("utf-8") match {
        case s if s.isEmpty => Set.empty
        case s => s.split (' ').map (_.toInt).map (portToNode).toSet
      }
    }
    catch {
      case th: akka.stream.StreamTcpException =>
        Set.empty
    }
  }

  def portToNode(port: Int): RoleName = roles.filter(address(_).port contains port).head

}
