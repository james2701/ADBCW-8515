#include "Implementation.hpp"
#include <iostream>

using namespace std;
//////////////////// Nested Loop Joins ////////////////////

std::vector<std::string> getQualifyingBusinessesIDsVector(Businesses const& b, float latMin,
																													float latMax, float longMin,
																													float longMax) {

	std::vector<std::string> QBIDV;
	for (int i = 0; i < b.ids.size(); i++) {
		if ((b.latitudes[i] <= latMax) && (b.latitudes[i] >= latMin) && (b.longitudes[i] <= longMax) && (b.longitudes[i] >= longMin)) {
			QBIDV.push_back(b.ids[i]);
		}
	}
	return QBIDV;
	// This function needs to find all businesses that have within the
	// specified latitude/longitude range and store their ids in the result vector
	std::cout << "function getQualifyingBusinessesIDsVector not implemented" << std::endl;
	throw std::logic_error("unimplemented");
}

std::vector<unsigned long>
performNestedLoopJoinAndAggregation(Reviews const& r, std::vector<std::string> const& qualifyingBusinessesIDs) {

	std::vector<unsigned long> hist(5);
	for (int i = 0; i < qualifyingBusinessesIDs.size(); i++) {
		for (int j = 0; j < r.business_ids.size(); j++) {
			if (r.business_ids[i] == qualifyingBusinessesIDs[j]) {
				hist[r.stars[i] - 1]++;
			}
		}
	}
	return hist;


	// This function needs to find all reviews that have business_ids in
	// the qualifyingBusinessesIDs vector and build a histogram over their stars
	// The return value is that histogram

	std::cout << "function performNestedLoopJoinAndAggregation not implemented" << std::endl;
	throw std::logic_error("unimplemented");
}

//////////////////// Hash Join ////////////////////

std::unordered_set<std::string> getQualifyingBusinessesIDs(Businesses const& b, float latMin,
																													 float latMax, float longMin,
																													 float longMax) {
	std::unordered_set<std::string> QBID;
	for (int i = 0; i < b.ids.size(); i++) {
		if ((b.latitudes[i] <= latMax) && (b.latitudes[i] >= latMin) && (b.longitudes[i] <= longMax) && (b.longitudes[i] >= longMin)) {
			QBID.insert(b.ids[i]);
		}
	}
	return QBID;
	
	// This function needs to find all businesses that have within the
	// specified latitude/longitude range and store their ids in the result set
	std::cout << "function getQualifyingBusinessesIDs not implemented" << std::endl;
	throw std::logic_error("unimplemented");
}

std::vector<unsigned long>
aggregateStarsOfQualifyingBusinesses(Reviews const& r,
																		 std::unordered_set<std::string> const& qualifyingBusinesses) {
	std::vector<unsigned long> hist(5);
	for (int i = 0; i < r.business_ids.size(); i++) {
		if (qualifyingBusinesses.count(r.business_ids[i]) {
			hist[r.stars[i] - 1]++;
		}
	}
	return hist;
	// This function needs to find all reviews that have business_ids in
	// the qualifyingBusinessesIDs vector and build a histogram over their stars
	// The return value is that histogram

	std::cout << "function aggregateStarsOfQualifyingBusinesses not implemented" << std::endl;
	throw std::logic_error("unimplemented");
}
