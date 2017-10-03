# Developer documentation
 
This document summarizes information relevant to Registry committers and contributors.  It includes information about
the development processes and policies as well as the tools we use to facilitate those.

---

Table of Contents

* <a href="#workflow-and-policies">Workflows</a>
    * <a href="#coding-guidelines">Coding Guidelines</a>
    * <a href="#contribute-code">Contribute code</a>
        * <a href="#create-pull-request">Create a pull request</a>
        * <a href="#merge-pull-request">Merge a pull request or patch</a>
    * <a href="#building">Build the code and run the tests</a>
* <a href="#best-practices">Best practices</a>
    * <a href="#best-practices-testing">Testing</a>
* <a href="#tools">Tools</a>
    * <a href="#code-repositories">Source code repositories (git)</a>
    * <a href="#issue-tracking">Issue tracking (JIRA)</a>
* <a href="#questions">Questions?</a>

---

# Workflows

This section explains how to perform common activities such as reporting a bug or merging a pull request.

<a name="coding-guidelines"></a>

## Coding Guidelines

### Basic

 1. Avoid cryptic abbreviations. Single letter variable names are fine in very short methods with few variables, otherwise make them informative.
 2. Clear code is preferable to comments. When possible make your naming so good you don't need comments. When that isn't possible comments should be thought of as mandatory, write them to be read.
 3. Logging, configuration, and public APIs are our "UI". Make them pretty, consistent, and usable.
 4. Maximum line length is 130.
 5. Don't leave TODOs in the code or FIXMEs if you can help it. Don't leave println statements in the code. TODOs should be filed as JIRAs.
 6. User documentation should be considered a part of any user-facing the feature, just like unit tests. Example REST apis should've accompanaying documentation.
 7. Tests should never rely on timing in order to pass.  
 8. Every unit test should leave no side effects, i.e., any test dependencies should be set during setup and clean during tear down.

### Java
 1. Apache license headers. Make sure you have Apache License headers in your files. 
 2. Tabs vs. spaces. We are using 4 spaces for indentation, not tabs. 
 3. Blocks. All statements after if, for, while, do, … must always be encapsulated in a block with curly braces (even if the block contains one statement):
     for (...) {
         ...
     }
 4. No wildcard imports. 
 5. No unused imports. Remove all unused imports.
 6. No raw types. Do not use raw generic types, unless strictly necessary (sometime necessary for signature matches, arrays).
 7. Suppress warnings. Add annotations to suppress warnings, if they cannot be avoided (such as “unchecked”, or “serial”).
 8. Comments.  Add JavaDocs to public methods or inherit them by not adding any comments to the methods. 
 9. logger instance should be upper case LOG.  
 10. When in doubt refer to existing code or <a href="http://google.github.io/styleguide/javaguide.html"> Java Coding Style </a> except line breaking, which is described above. 
  

### Logging

 1. Please take the time to assess the logs when making a change to ensure that the important things are getting logged and there is no junk there.
 2. There are six levels of logging TRACE, DEBUG, INFO, WARN, ERROR, and FATAL, they should be used as follows.
    
    ``` 
    2.1 INFO is the level you should assume the software will be run in. 
     INFO messages are things which are not bad but which the user will definitely want to know about
     every time they occur.
    
    2.2 TRACE and DEBUG are both things you turn on when something is wrong and you want to figure out 
    what is going on. DEBUG should not be so fine grained that it will seriously effect the performance 
    of the server. TRACE can be anything. Both DEBUG and TRACE statements should be 
    wrapped in an if(logger.isDebugEnabled) if an expensive computation in the argument list of log method call.
    
    2.3 WARN and ERROR indicate something that is bad. Use WARN if you aren't totally sure it is bad,
     and ERROR if you are.
    
    2.4 Use FATAL only right before calling System.exit().
    ```
 3. Logging statements should be complete sentences with proper capitalization that are written to be read by a person not necessarily familiar with the source code. 
 4. String appending using StringBuilders should not be used for building log messages. 
    Formatting should be used. For ex:
    LOG.debug("Loaded class [{}] from jar [{}]", className, jarFile);
 

<a name="contribute-code"></a>

## Contribute code

<a name="create-pull-request"></a>

### Create a pull request

Pull requests should be done against the read-only git repository at
[https://github.com/hortonworks/registry](https://github.com/hortonworks/registry).

Take a look at [Creating a pull request](https://help.github.com/articles/creating-a-pull-request).  In a nutshell you
need to:

1. [Fork](https://help.github.com/articles/fork-a-repo) the Registry GitHub repository at
   [https://github.com/hortonworks/registry/](https://github.com/hortonworks/registry/) to your personal GitHub
   account.  See [Fork a repo](https://help.github.com/articles/fork-a-repo) for detailed instructions.
2. Commit any changes to your fork.
3. Send a [pull request](https://help.github.com/articles/creating-a-pull-request) to the Registry GitHub repository
   that you forked in step 1.  If your pull request is related to an existing Github issue  -- for instance, because
   you reported a bug report via Github issue earlier -- then prefix the title of your pull request with the corresponding 
   Github issue number (e.g. `ISSUE-123: ...`).

You may want to read [Syncing a fork](https://help.github.com/articles/syncing-a-fork) for instructions on how to keep
your fork up to date with the latest changes of the upstream  `Registry` repository.

### The format of pull request

The title of pull request must be standardized as follows:

```
ISSUE-XXX: Title matching exactly the title of Github issue
```

The text immediately following the Github issue number (ISSUE-XXX: ) must be an exact transcription of the title of Github issue, not the a summary of the contents of the patch.

If the title of Github issue does not accurately describe what the patch is addressing, the title of Github issue must be modified, and then copied to the Git commit message.

And the body of pull request is encouraged to be standardized as follows:

```
- An optional, bulleted (+, -, ., *), summary of the contents of 
- the patch. The goal is not to describe the contents of every file,
- but rather give a quick overview of the main functional areas 
- addressed by the patch.
```

A summary with the contents of the patch is optional but strongly encouraged if the patch is large and/or the Github title is not expressive enough to describe what the patch is doing. This text must be bulleted using one of the following bullet points (+, -, ., ). There must be at last a 1 space indent before the bullet char, and exactly one space between bullet char and the first letter of the text. Bullets are not optional, but **required**.

<a name="merge-pull-request"></a>

### Merge a pull request or patch

To pull in a merge request you should generally follow the command line instructions sent out by GitHub.

1. Create remote repo called upstream with Registry [repo](https://github.com/hortonworks/registry.git)

        $ git remote add upstream https://github.com/hortonworks/registry.git
        $ git fetch upstream
        
2. Create a local branch for integrating and testing the pull request.  You may want to name the branch according to 
   Registry Issue associated with the pull request (example: `ISSUE-1234`).

        $ git checkout -b <local_test_branch>  upstream/master # e.g. git checkout -b ISSUE-1234

3. Merge the pull request into your local test branch.

        $ git pull <remote_repo_url> <remote_branch>

4.  Assuming that the pull request merges without any conflicts:

5. Run any sanity tests that you think are needed as mentioned in [build the code and run the tests](#build-the-code-and-run-the-tests).

6. Once you are confident that everything is ok, you can merge your local test branch into your local `master` branch,
   and push the changes back to remote repo.

        # Pull request looks ok, change log was updated, etc.  We are ready for pushing.
        $ git checkout master
        $ git merge <local_test_branch>  # e.g. git merge ISSUE-1234

        # At this point our local master branch is ready, so now we will push the changes
        # to the official repo.  
        $ git push origin  HEAD:refs/heads/master 

7. The last step is updating the corresponding Github issue with respective resolution.
   
<a name="building"></a>

# Build the code and run the tests

## Prerequisites
Make sure you have maven 3.2.5 or higher and JDK 1.8 or higher.

## Building

The following commands must be run from the top-level directory.

`mvn clean install`

If you wish to skip the unit tests you can do this by adding `-DskipTests` to the command line. 

## Create a distribution (packaging)

You can create a _distribution_ as follows.

    # First, build the code.
    # Pivot module required for the dist package to be built , Its not run as part of default build.
    # To build pivot along with all the other modules. 
    $ mvn clean install -Pall 

    # Create the binary distribution.
    $ cd registry-dist && mvn package

You can also use the maven `dist` profile to build the code and create the distribution in one step.

    $ mvn clean install -P dist 

The binaries will be created at:

    registry-dist/target/hortonworks-registry-<version>.pom
    registry-dist/target/hortonworks-registry-<version>.tar.gz
    registry-dist/target/hortonworks-registry-<version>.zip

including corresponding `*.asc` digital signature files.

After running `mvn package` you may be asked to enter your GPG/PGP credentials (once for each binary file, in fact).
This happens because the packaging step will create `*.asc` digital signatures for all the binaries, and in the workflow
above _your_ GPG private key will be used to create those signatures.

You can verify whether the digital signatures match their corresponding files:

    # Example: Verify the signature of the `.tar.gz` binary.
    $ gpg --verify registry-dist/target/hortonworks-registry-<version>.tar.gz.asc
