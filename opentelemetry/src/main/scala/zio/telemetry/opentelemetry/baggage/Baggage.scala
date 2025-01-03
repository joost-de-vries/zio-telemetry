package zio.telemetry.opentelemetry.baggage

import io.opentelemetry.api.baggage.{Baggage => Baggaje, BaggageBuilder, BaggageEntryMetadata}
import io.opentelemetry.context.Context
import zio._
import zio.telemetry.opentelemetry.baggage.propagation.BaggagePropagator
import zio.telemetry.opentelemetry.context.{ContextStorage, IncomingContextCarrier, OutgoingContextCarrier}

import scala.jdk.CollectionConverters._

trait Baggage { self =>

  /**
   * Extracts the baggage data from carrier `C` into the current context.
   *
   * @param propagator
   *   implementation of [[zio.telemetry.opentelemetry.baggage.propagation.BaggagePropagator]]
   * @param carrier
   *   mutable data from which the parent span is extracted
   * @param trace
   * @tparam C
   *   carrier
   * @return
   */
  def extract[C](
    propagator: BaggagePropagator,
    carrier: IncomingContextCarrier[C]
  )(implicit trace: Trace): UIO[Unit]

  /**
   * Extracts the baggage data from carrier `C` into the current context. The context will be scoped to the provided
   * scope.
   *
   * @param propagator
   *   implementation of [[zio.telemetry.opentelemetry.baggage.propagation.BaggagePropagator]]
   * @param carrier
   *   mutable data from which the parent span is extracted
   * @param trace
   * @tparam C
   *   carrier
   * @return
   */
  def extractScoped[C](
    propagator: BaggagePropagator,
    carrier: IncomingContextCarrier[C]
  )(implicit trace: Trace): URIO[Scope, Unit]

  /**
   * Gets the value by a given name.
   *
   * @param name
   * @param trace
   * @return
   *   value
   */
  def get(name: String)(implicit trace: Trace): UIO[Option[String]]

  /**
   * Gets all values.
   *
   * @param trace
   * @return
   *   all values
   */
  def getAll(implicit trace: Trace): UIO[Map[String, String]]

  /**
   * Gets all values accompanied by metadata.
   *
   * @param trace
   * @return
   */
  def getAllWithMetadata(implicit trace: Trace): UIO[Map[String, (String, String)]]

  /**
   * Gets the baggage from current context.
   *
   * @param trace
   * @return
   */
  def getCurrentBaggageUnsafe(implicit trace: Trace): UIO[Baggaje]

  /**
   * Injects the baggage data from the current context into carrier `C`.
   *
   * @param propagator
   *   implementation of [[zio.telemetry.opentelemetry.baggage.propagation.BaggagePropagator]]
   * @param carrier
   *   mutable data from which the parent span is extracted
   * @param trace
   * @tparam C
   *   carrier
   * @return
   */
  def inject[C](
    propagator: BaggagePropagator,
    carrier: OutgoingContextCarrier[C]
  )(implicit trace: Trace): UIO[Unit]

  /**
   * Removes the name/value by a given name.
   *
   * @param name
   * @param trace
   * @return
   */
  def remove(name: String)(implicit trace: Trace): UIO[Unit]

  /**
   * Removes the name/value by a given name. The context will be scoped to the provided scope.
   *
   * @param name
   * @param trace
   * @return
   */
  def removeScoped(name: String)(implicit trace: Trace): URIO[Scope, Unit]

  /**
   * Sets the new value for a given name.
   *
   * @param name
   * @param value
   * @param trace
   * @return
   */
  def set(name: String, value: String)(implicit trace: Trace): UIO[Unit]

  /**
   * Sets the new value for a given name. The context will be scoped to the provided scope.
   *
   * @param name
   * @param value
   * @param trace
   * @return
   */
  def setScoped(name: String, value: String)(implicit trace: Trace): URIO[Scope, Unit]

  /**
   * Sets the new value and metadata for a given name.
   *
   * @param name
   * @param value
   * @param metadata
   *   opaque string
   * @param trace
   * @return
   */
  def setWithMetadata(name: String, value: String, metadata: String)(implicit trace: Trace): UIO[Unit]

  /**
   * Sets the new value and metadata for a given name. The context will be scoped to the provided scope.
   *
   * @param name
   * @param value
   * @param metadata
   *   opaque string
   * @param trace
   * @return
   */
  def setWithMetadataScoped(name: String, value: String, metadata: String)(implicit trace: Trace): URIO[Scope, Unit]
}

object Baggage {

  def live(logAnnotated: Boolean = false): URLayer[ContextStorage, Baggage] =
    ZLayer {
      for {
        ctxStorage <- ZIO.service[ContextStorage]
      } yield new Baggage { self =>
        override def getCurrentBaggageUnsafe(implicit trace: Trace): UIO[Baggaje] =
          injectLogAnnotations *>
            getCurrentContext.map(Baggaje.fromContext)

        override def get(name: String)(implicit trace: Trace): UIO[Option[String]] =
          injectLogAnnotations *>
            getCurrentBaggageUnsafe.map(baggage => Option(baggage.getEntryValue(name)))

        override def getAll(implicit trace: Trace): UIO[Map[String, String]] =
          injectLogAnnotations *>
            getCurrentBaggageUnsafe.map(_.asMap().asScala.toMap.map { case (k, v) => k -> v.getValue })

        override def getAllWithMetadata(implicit trace: Trace): UIO[Map[String, (String, String)]] =
          injectLogAnnotations *>
            getCurrentBaggageUnsafe.map(
              _.asMap().asScala.toMap.map { case (k, v) => (k, (v.getValue, v.getMetadata.getValue)) }
            )

        override def set(name: String, value: String)(implicit trace: Trace): UIO[Unit] =
          ZIO.scoped[Any](setScoped(name, value))

        override def setScoped(name: String, value: String)(implicit trace: Trace): URIO[Scope, Unit] =
          injectLogAnnotations *> modifyBuilder(_.put(name, value)).unit

        override def setWithMetadata(name: String, value: String, metadata: String)(implicit trace: Trace): UIO[Unit] =
          ZIO.scoped[Any](setWithMetadataScoped(name, value, metadata))

        override def setWithMetadataScoped(name: String, value: String, metadata: String)(implicit
          trace: Trace
        ): URIO[Scope, Unit] =
          injectLogAnnotations *> modifyBuilder(_.put(name, value, BaggageEntryMetadata.create(metadata))).unit

        override def remove(name: String)(implicit trace: Trace): UIO[Unit] =
          ZIO.scoped[Any](removeScoped(name))

        override def removeScoped(name: String)(implicit trace: Trace): URIO[Scope, Unit] =
          injectLogAnnotations *> modifyBuilder(_.remove(name)).unit

        override def inject[C](
          propagator: BaggagePropagator,
          carrier: OutgoingContextCarrier[C]
        )(implicit trace: Trace): UIO[Unit] =
          for {
            _   <- injectLogAnnotations
            ctx <- getCurrentContext
            _   <- ZIO.succeed(propagator.instance.inject(ctx, carrier.kernel, carrier))
          } yield ()

        override def extract[C](
          propagator: BaggagePropagator,
          carrier: IncomingContextCarrier[C]
        )(implicit trace: Trace): UIO[Unit] =
          ZIO.scoped[Any](extractScoped(propagator, carrier))

        override def extractScoped[C](
          propagator: BaggagePropagator,
          carrier: IncomingContextCarrier[C]
        )(implicit trace: Trace): URIO[Scope, Unit] =
          injectLogAnnotations *>
            ZIO.uninterruptible {
              modifyContext(ctx => propagator.instance.extract(ctx, carrier.kernel, carrier)).unit
            }

        private def getCurrentContext(implicit trace: Trace): UIO[Context] =
          ctxStorage.get

        private def modifyBuilder(body: BaggageBuilder => BaggageBuilder)(implicit trace: Trace): URIO[Scope, Context] =
          modifyContext { ctx =>
            body(Baggaje.fromContext(ctx).toBuilder)
              .build()
              .storeInContext(ctx)
          }

        private def modifyContext(body: Context => Context)(implicit trace: Trace): URIO[Scope, Context] =
          ctxStorage.updateAndGet(body)

        private def injectLogAnnotations(implicit trace: Trace): UIO[Unit] =
          ZIO
            .when(logAnnotated) {
              for {
                annotations            <- ZIO.logAnnotations
                annotationsWithMetadata = annotations.map { case (k, v) =>
                                            (k, (v, BaggageEntryMetadata.create("zio log annotation")))
                                          }
                current                <- getCurrentContext
                                            .map(Baggaje.fromContext)
                                            .map(_.asMap().asScala.toMap.map { case (k, v) => (k, (v.getValue, v.getMetadata)) })
                _                      <- ZIO.scoped[Any](modifyBuilder { builder =>
                                            (annotationsWithMetadata ++ current).foreach { case (k, (v, m)) => builder.put(k, v, m) }
                                            builder
                                          })
              } yield ()
            }
            .unit
      }
    }

}
