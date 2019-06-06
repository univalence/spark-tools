
# Local setup

You will need to install Jekyll, which is a ruby framework that you can
get by using `gem` (the Ruby dependency manager).
To do so, you first need to install Ruby.
On OSX platform, the default Ruby with download on incompatible version
of Jekyll. So use Homebrew to get a better version.

    $ brew install ruby

then use `gem` to get Jekyll.

    $ sudo gem install jekyll

# Build

To build the Web site:

    $ sbt makeMicrosite

# Local visualization

Once the Web site is built, go to `site/target/site` and launch Jekyll.

    $ jekyll serve

The Web site will open on http://127.0.0.1:4000/spark-tools/.

# Publish

    $ sbt publishMicrosite
