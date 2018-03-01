protoc="/Users/paul/Documents/code_dir/cfop3_search/trunk/third-party/protoc/protoc-2.5.0.sh"

current_dir=`pwd`
proto_src="$current_dir/src/main/proto"
java_out="$current_dir/src/main/java"
proto_file="$current_dir/src/main/proto/store.proto"

$protoc -I=$proto_src --java_out=$java_out $proto_file
