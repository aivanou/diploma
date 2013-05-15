libraryDependencies += "junit" % "junit" % "3.8.1"

libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.7"

libraryDependencies += "org.apache.mahout" % "mahout-math" % "0.7"

libraryDependencies += "org.apache.mahout" % "mahout-collections" % "1.0"

libraryDependencies += "org.apache.mahout" % "mahout-utils" % "0.5"

libraryDependencies  ++= Seq(
            // other dependencies here
            // pick and choose:
            "org.scalanlp" %% "breeze-math" % "0.1",
            "org.scalanlp" %% "breeze-learn" % "0.1",
            "org.scalanlp" %% "breeze-process" % "0.1",
            "org.scalanlp" %% "breeze-viz" % "0.1"
)
