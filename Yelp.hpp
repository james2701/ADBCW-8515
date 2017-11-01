#include <memory>
#include <odb/core.hxx>
#include <odb/lazy-ptr.hxx>
#include <set>
#include <string>
#include <vector>

#pragma db view
class StarCount{
public:
	int stars;
	int count;
};

#pragma db view query("select top 1 text, last_elapsed_time from sys.dm_exec_query_stats cross apply sys.dm_exec_sql_text(sql_handle) order by last_execution_time desc")
class LastQueryTime{
public:
	std::string text;
	long elapsed_time;
};

// ---------------------------------------------
// No need to change anything above this line
// ---------------------------------------------

class review;


#pragma db object
class user{
public:
	#pragma db id 
	std::string id;
	std::string name;
	#pragma db value_not_null inverse(user_id)
	std::vector<std::weak_ptr<review> > review_;
}

#pragma db object
class business{
public:
	#pragma db id
	std::string id;
	std::string name;
	#pragma db value_not_null inverse(business_id)
	std::vector<std::weak_ptr<review> > review_
	#pragma db value_not_null inverse(business_id)
	std::vector<std::weak_ptr<hours> > hours_id
}

#pragma db object
class review{
public:
	#pragma db id
	std::string id;
	#pragma db not_null
	std::shared_ptr<user> user_id;
	#pragma db not_null
	std::shared_ptr<business> business_id;
}

#pragma db object
class hours{
public:
	#pragma db id
	std::string id;
	#pragma db not_null
	std::shared_ptr<business> business_id;
}
