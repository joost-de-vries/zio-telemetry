package zio.telemetry.opentelemetry.context

import io.opentelemetry.context.{Context, ContextKey}
import zio.{Scope, ZIO}
import zio.test.{Spec, TestEnvironment, ZIOSpecDefault, assertTrue}

object ContextStorageTest extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("zio opentelemetry")(
      suite("ContextStorage.native")(
        test("set should restore context after scope") {
          for {
            contextStorage <- ZIO.service[ContextStorage]
            before          = Context.current()
            updated         = before.`with`(key, "local value")
            withinScope    <- ZIO.scoped[Any](contextStorage.set(updated) *> contextStorage.get)
            after           = Context.current()
          } yield assertTrue(
            withinScope.get(key) == "local value",
            after.get(key) == null,
            after == before
          )
        },
        test("getAndSet should restore context after scope") {
          for {
            contextStorage           <- ZIO.service[ContextStorage]
            before                    = Context.current()
            updated                   = before.`with`(key, "local value")
            result                   <- ZIO.scoped[Any](contextStorage.getAndSet(updated) <*> contextStorage.get)
            (likeBefore, withinScope) = result
            after                     = Context.current()
          } yield assertTrue(
            likeBefore == before,
            withinScope.get(key) == "local value",
            after.get(key) == null,
            after == before
          )
        },
        test("updateAndGet should restore context after scope") {
          for {
            contextStorage              <- ZIO.service[ContextStorage]
            before                       = Context.current()
            result                      <-
              ZIO.scoped[Any] {
                contextStorage.updateAndGet(c => c.`with`(key, "local value")) <*> contextStorage.get
              }
            (updated, afterUpdateAndGet) = result
            after                        = Context.current()
          } yield assertTrue(
            updated.get(key) == "local value",
            updated == afterUpdateAndGet,
            after.get(key) == null,
            after == before
          )
        },
        test("locally should restore context after scope") {
          for {
            contextStorage <- ZIO.service[ContextStorage]
            before          = Context.current()
            updated         = before.`with`(key, "local value")
            value          <- contextStorage.locally(updated) {
                                contextStorage.get.map(_.get(key))
                              }
            after           = Context.current()
          } yield assertTrue(
            value == "local value",
            after.get(key) == null,
            after == before
          )
        },
        test("locallyScoped should restore context after scope") {
          for {
            contextStorage <- ZIO.service[ContextStorage]
            before          = Context.current()
            updated         = before.`with`(key, "local value")
            withinScope    <- ZIO.scoped[Any] {
                                contextStorage.locallyScoped(updated) *> contextStorage.get
                              }
            after           = Context.current()
          } yield assertTrue(
            after.get(key) == null,
            withinScope.get(key) == "local value",
            after == before
          )
        }
      ).provide(ContextStorage.native)
    )

  private val key = ContextKey.named[String]("local-only")

}
