package com.ajjpj.simpleakkadowning.util

import akka.actor.Actor
import akka.cluster.{Cluster, MemberStatus}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer

import scala.concurrent.Await

/**
  * This class exposes some cluster state via HTTP to facilitate testing
  */
class ClusterHttpInspector(httpPort: Int) extends Actor {
  private val cluster = Cluster.get(context.system)
  private val routes = {
    import Directives._

    pathPrefix("cluster-members") {
      path("up") { complete {
        // TODO: the member status UP does not mean that a member is not reachable
        cluster.state.members.diff(cluster.state.unreachable).map(_.address.port.get).mkString(" ")
      }} ~
      path("unreachable") { complete {
        cluster.state.unreachable.map(_.address.port.get).mkString(" ")
      }}
    }
  }

  import context.dispatcher
  private implicit val mat = ActorMaterializer()

  private val fServerBinding =
    Http(context.system)
      .bindAndHandle(routes, "localhost", httpPort)


  override def postStop () = {
    import scala.concurrent.duration._
    super.postStop ()
    fServerBinding.foreach(sb => Await.ready (sb.unbind(), 5.seconds))
  }

  override def receive = Actor.emptyBehavior

}
