
#pragma once

#include <string>

inline char* AllocateCString(const std::string& str)
{
	auto const cstr = new char[str.length() + 1];
	std::memcpy(cstr, str.c_str(), str.length() + 1);

	return cstr;
}

inline void FreeCString(const char* const cstr)
{
	delete[] cstr;
}

