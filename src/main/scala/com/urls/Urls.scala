package com.urls

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.event.LoggingReceive
import akka.util.Timeout
import better.files._
import com.urls.FileReader.CheckForFiles
import com.urls.UrlProcessor.{ProcessUrls, UrlDone, UrlFailed, WriteResults}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success}

object Urls extends App {
  val system = ActorSystem("url-word-count")

  val inputDir = "temp" / "urls" / "input"

  val urlsToRead = Seq(
    "http://www.google.com",
    "https://en.wikipedia.org/wiki/Bubble_sort",
    "https://en.wikipedia.org/wiki/Sorting_algorithm",
    "https://en.wikipedia.org/wiki/Algorithm",
    "https://en.wikipedia.org/wiki/Calculation"
  )
  val file1 = inputDir.createChild("urls-1", asDirectory = false, createParents = true)
  val file2 = inputDir.createChild("urls-2", asDirectory = false, createParents = true)
  urlsToRead.foreach { url =>
    file1.appendLine(url)
    file2.appendLine(url)
  }
  system.actorOf(FileReader.props(inputDir), s"FileReader-${System.currentTimeMillis()}")

  Await.result(system.whenTerminated, 5.minutes)
}

object UrlProcessor {
  case class ProcessUrls(urls: List[String])
  case class UrlDone(url: String, wordCount: Int)
  case class UrlFailed(url: String, t: Throwable)
  case object WriteResults

  def props(input: File) = Props(classOf[UrlProcessor], input.nameWithoutExtension)
}
class UrlProcessor(inputFile: String) extends Actor with ActorLogging {
  val OUTPUT_DIR = ("temp" / "urls" / "output").createDirectories()
  implicit val ec = context.system.dispatcher

  var processingUrls: List[(String, Int)] = List.empty
  var urlCount = 0

  override def receive = LoggingReceive {

    case ProcessUrls(urls) =>
      urlCount = urls.size
      val fs = urls.map { url =>
        Future(countWords(url)).map(count => (url, count)).andThen {
          case Success((resultUrl, wordCount)) => self ! UrlDone(resultUrl, wordCount)
          case Failure(t) => self ! UrlFailed(url, t)
        }
      }
      Future.sequence(fs)

    case UrlDone(url, wordCount) =>
      log.debug(s"processed $url from $inputFile - $wordCount words")
      updateWithCount(url, wordCount)

    case UrlFailed(url, t) =>
      updateWithCount(url, -1)
      log.warning(s"failure with processing url $url of file $inputFile - ${t.getMessage}")

    case WriteResults =>
      val outputFile = OUTPUT_DIR.createChild(s"$inputFile-output")
      processingUrls.map { case (url, wordCount) =>
        outputFile.appendLine(s"$url - $wordCount")
      }
      context.stop(self)
    case _ =>
  }

  override def postStop() = log.info(s"UrlProcessor for $inputFile is down")

  def updateWithCount(url: String, wordCount: Int) = {
    processingUrls = processingUrls :+ (url, wordCount)

    if (processingUrls.lengthCompare(urlCount) == 0) {
      log.info(s"done with file $inputFile")
      self ! WriteResults
    }
  }

  def countWords(url: String): Int = Source.fromURL(url).getLines().map(_.split(" ").length).sum
}

object FileReader {
  case object CheckForFiles
  def props(inputDir: File) = Props(classOf[FileReader], inputDir)
}
class FileReader(inputDir: File) extends Actor with ActorLogging {
  import context._
  implicit val timeout = Timeout(5.minutes)

  system.scheduler.schedule(5.seconds, 1.minute, self, CheckForFiles)

  def readInputFiles() = {
    val files = inputDir.children.toList
    val input = files.map(file => (file, file.contentAsString.split("\r\n").toList))
    files.foreach(_.delete())
    input
  }

  override def receive = LoggingReceive {
    case CheckForFiles =>
      val inputFiles = readInputFiles()
      inputFiles.foreach { case (input, urls) =>
        log.info(s"found file $input with ${urls.size} urls to process...")
        system.actorOf(UrlProcessor.props(input), s"UrlProcessorActor-${input.nameWithoutExtension}") ! ProcessUrls(urls)
      }

    case _ =>
  }
}