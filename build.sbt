name := "urls"

version := "0.1"

scalaVersion := "2.12.4"
val akkaActorVer = "2.5.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka"          %% "akka-actor"                 % akkaActorVer,
  "com.typesafe.akka"          %% "akka-testkit"               % akkaActorVer % "test",
  "com.github.pathikrit"       %% "better-files"               % "3.4.0",
)