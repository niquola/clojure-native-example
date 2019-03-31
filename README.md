## Clojure native with pg and httpkit

Install graalvm

```

export PATH="$HOME/graalvm/Contents/Home/bin:$PATH"
export GRAALVM_HOME=$HOME/graalvm/Contents/Home
export JAVA_HOME=~/graalvm

make build
export PGHOST= 
export PGPORT= 
export PGPASSWORD=
export PGUSER=

#run
target/cloud-extra

open localhost:8585


```
