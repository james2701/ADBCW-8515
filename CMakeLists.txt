cmake_minimum_required(VERSION 3.5)

add_custom_command(OUTPUT review.csv COMMAND bcp "select business_id, stars from review" queryout review.csv -dyelp -Slocalhost -U sa -PAdvancedDB17 -c)
add_custom_command(OUTPUT business.csv COMMAND bcp "select id, latitude, longitude from business" queryout business.csv -dyelp -Slocalhost -U sa -PAdvancedDB17 -c)

find_package (Threads)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)

add_executable(ADBCoursework2 ADBCoursework2.cpp Implementation.cpp)
SET_SOURCE_FILES_PROPERTIES(ADBCoursework2.cpp OBJECT_DEPENDS "review.csv;business.csv")
set_property(TARGET ADBCoursework2 PROPERTY CXX_STANDARD 14)
target_link_libraries(ADBCoursework2 Threads::Threads)
