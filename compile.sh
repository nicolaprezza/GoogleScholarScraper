find ./src -name "*.java" > sources
javac @sources -d ./bin -cp ./lib/*
rm ./sources
