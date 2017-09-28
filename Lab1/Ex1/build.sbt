name := "WordFreqCounts"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

resourceDirectory in Compile := baseDirectory.value / "resources"

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

