include_directories(include)

add_library(MyCoolDB
        include/DataBase.h src/DataBase.cpp
        include/ParserSQL.h src/ParserSQL.cpp
        )
