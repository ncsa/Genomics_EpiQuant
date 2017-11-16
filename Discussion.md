# Features to add to full version:
* be able to automatically start with some terms included in the model
* Backward and forward thresholds need to be the same, otherwise the procedure can become an infinite loop
* Add an option to specify the maximum number of iterations
* Check to see if output location exists at the very beginning of the program
* Determine whether it is necessary to skip removed SNPs when performing steps:
  * if this part of the model building steps can be removed, the function becomes much simpler

# Architecture and Design Ideas:

## Command-line programs:

To specify properties of command-line programs, the GATK code uses a system of special annotations (marked by '@' signs in Java)
   For example:
```
@CommandLineProgramProperties(
    summary = "Prints reads from the provided file(s) with corresponding reference bases (if a reference is provided) to the specified output file (or STDOUT if none specified)",
    oneLineSummary = "Print reads with reference context",
    programGroup = ExampleProgramGroup.class,
    omitFromCommandLine = true
)
public final class ExampleReadWalkerWithReference extends ReadWalker {
...and so on
```

I assume they do this so that they can later grab all classes that have command line properties so they can print usage information. However, I am not sure why they are using Annotations instead of just having a methodless class called, say, CommandLineProperties, that must be included as a field in any class that extends an CommandLineProgram abstract class?

Maybe this is hard/impossible to do in Java since abstract classes cannot force field "implementation". One could just have the default values found in the abstract class and make sure force users override them, although this may not look good.

After some experimentation, I think I have figured it out. It comes down to two things:
  1. They are wanting to store configuration info (fields) with CommandLinePrograms at the time when the class is created
  2. Java does not allow fields to be abstract, and interfaces do not have fields
  3. They have to play with annotations to get this functionality
  
With scala, we can just mix in (or extend) a trait, which is like an interface that can have abstract fields as well as methods, or have an abstract class that contains a case class with the properties contained within it.

I think we should go with a trait, because there is no need to have a case class layer in between the concrete class and the fields we want (unless we end up pattern matching against the properties case class, which we may)

```
trait ProgramProperties {
    val name: String
    val id: String
    val number: Int
    val exampleProgram: Boolean
}

abstract class AbstractProgram extends ProgramProperties {
    def printSomething()
}

class ConcreteProgram extends AbstractProgram {
    val name = "ConcreteProgram"
    val id = "71GG9"
    val number = 77
    val exampleProgram = false

    def printSomething() = println("Look, I printed something")
}

object Implementation extends App {
    val newProgram = new ConcreteProgram

    println(newProgram.name)
    println(newProgram.printSomething())
}
```
