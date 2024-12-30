package zio.telemetry.opentelemetry.context

import io.opentelemetry.context.{Scope => OtelScope}
import io.opentelemetry.context.Context
import zio._

/**
 * An interface crafted to offer abstraction from the approach of context propagation.
 */
sealed trait ContextStorage {

  def get(implicit trace: Trace): UIO[Context]

  def set(context: Context)(implicit trace: Trace): URIO[Scope, Unit]

  def getAndSet(context: Context)(implicit trace: Trace): URIO[Scope, Context]

  def updateAndGet(f: Context => Context)(implicit trace: Trace): URIO[Scope, Context]

  def locally[R, E, A](context: Context)(zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A]

  def locallyScoped(context: Context)(implicit trace: Trace): ZIO[Scope, Nothing, Unit]
}

private[opentelemetry] object ContextStorage {

  /**
   * The implementation that uses [[zio.FiberRef]] as a storage for [[io.opentelemetry.context.Context]]
   *
   * @param ref
   */
  final class ZIOFiberRef(private[zio] val ref: FiberRef[Context]) extends ContextStorage {

    override def get(implicit trace: Trace): UIO[Context] =
      ref.get

    override def set(context: Context)(implicit trace: Trace): UIO[Unit] =
      ref.set(context)

    override def getAndSet(context: Context)(implicit trace: Trace): UIO[Context] =
      ref.getAndSet(context)

    override def updateAndGet(f: Context => Context)(implicit trace: Trace): UIO[Context] =
      ref.updateAndGet(f)

    override def locally[R, E, A](context: Context)(zio: ZIO[R, E, A])(implicit
      trace: Trace
    ): ZIO[R, E, A] =
      ref.locally(context)(zio)

    override def locallyScoped(context: Context)(implicit trace: Trace): ZIO[Scope, Nothing, Unit] =
      ref.locallyScoped(context)
  }

  /**
   * The implementation that uses [[java.lang.ThreadLocal]] as a storage for [[io.opentelemetry.context.Context]]
   */
  object Native extends ContextStorage {

    override def get(implicit trace: Trace): UIO[Context] =
      ZIO.succeed(Context.current())

    override def set(context: Context)(implicit trace: Trace): URIO[Scope, Unit] =
      makeCurrentScoped(context).unit

    private def makeCurrentScoped(context: Context): URIO[Scope, OtelScope] =
      ZIO.fromAutoCloseable(ZIO.succeed(context.makeCurrent()))

    override def getAndSet(context: Context)(implicit trace: Trace): URIO[Scope, Context] =
      (get <* makeCurrentScoped(context)).uninterruptible

    override def updateAndGet(f: Context => Context)(implicit trace: Trace): URIO[Scope, Context] =
      get.flatMap { old =>
        val updated = f(old)
        makeCurrentScoped(updated).as(updated)
      }.uninterruptible

    override def locally[R, E, A](context: Context)(zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
      ZIO.scoped[R] {
        for {
          _      <- locallyScoped(context)
          result <- zio
        } yield result
      }

    override def locallyScoped(context: Context)(implicit trace: Trace): ZIO[Scope, Nothing, Unit] =
      ZIO.acquireRelease(get <* set(context))(old => set(old)).unit
  }

  /**
   * The default one. Use in cases where you do not use automatic instrumentation. Uses [[zio.FiberRef]] as a
   * [[ContextStorage]].
   */
  val fiberRef: ULayer[ContextStorage] =
    ZLayer.scoped(
      FiberRef
        .make[Context](Context.root())
        .map(new ZIOFiberRef(_))
    )

  /**
   * Uses OpenTelemetry's context storage which is backed by a [[java.lang.ThreadLocal]]. This makes sense only if
   * [[https://github.com/open-telemetry/opentelemetry-java-instrumentation OTEL instrumentation agent]] is used.
   */
  val native: ULayer[ContextStorage] =
    ZLayer.succeed(Native)

}
