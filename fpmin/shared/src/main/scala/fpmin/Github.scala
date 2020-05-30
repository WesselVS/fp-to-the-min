package fpmin

import java.io.IOException
import java.util.concurrent.TimeUnit
import scala.io.Source

import zio.ZIO
import zio.blocking.Blocking
import zio.blocking._
import zio.clock.Clock
import zio.duration.Duration
class Github() {

  /**
   * Downloads a file from Github.
   */
  def unsafeDownload(slug: String, file: String): ZIO[Blocking with Clock, IOException, String] =
    unsafeOpen(slug, file).bracket(s => ZIO.succeed(s.close()), source =>
      ZIO.effect(source.getLines().mkString("\n")).refineOrDie { case i: IOException => i}
    )

  /**
   * Opens a file from Github, so that it can be streamed or downloaded.
   */
  private def unsafeOpen(slug: String, file: String): ZIO[Blocking with Clock, IOException, Source] =
    effectBlocking { Source.fromURL(Github.downloadUrl(slug, file)) }
      .refineOrDie { case i: IOException => i}
      .timeoutFail(new IOException("timed out"))(Duration(5, TimeUnit.MINUTES))

}
object Github {
  private def downloadUrl(slug: String, file: String): String =
    s"https://github.com/${slug}/raw/master/${file}"
}
