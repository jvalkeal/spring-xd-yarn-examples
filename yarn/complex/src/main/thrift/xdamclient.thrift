namespace  java   org.springframework.yarn.examples.gen

service XdAdmin {
    bool setRunningCount(1: i32 count)
    bool setGroupRunningCount(1: i32 count, 2: string group)
    bool shutdown()
}
