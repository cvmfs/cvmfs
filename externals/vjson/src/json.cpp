#include <memory.h>
#include "json.h"

// true if character represent a digit
#define IS_DIGIT(c) (c >= '0' && c <= '9')

// convert string to integer
char *atoi(char *first, char *last, int *out)
{
	int sign = 1;
	if (first != last)
	{
		if (*first == '-')
		{
			sign = -1;
			++first;
		}
		else if (*first == '+')
		{
			++first;
		}
	}

	int result = 0;
	for (; first != last && IS_DIGIT(*first); ++first)
	{
		result = 10 * result + (*first - '0');
	}
	*out = result * sign;

	return first;
}

// convert hexadecimal string to unsigned integer
char *hatoui(char *first, char *last, unsigned int *out)
{
	unsigned int result = 0;
	for (; first != last; ++first)
	{
		int digit;
		if (IS_DIGIT(*first))
		{
			digit = *first - '0';
		}
		else if (*first >= 'a' && *first <= 'f')
		{
			digit = *first - 'a' + 10;
		}
		else if (*first >= 'A' && *first <= 'F')
		{
			digit = *first - 'A' + 10;
		}
		else
		{
			break;
		}
		result = 16 * result + digit;
	}
	*out = result;

	return first;
}

// convert string to floating point
char *atof(char *first, char *last, float *out)
{
	// sign
	float sign = 1;
	if (first != last)
	{
		if (*first == '-')
		{
			sign = -1;
			++first;
		}
		else if (*first == '+')
		{
			++first;
		}
	}

	// integer part
	float result = 0;
	for (; first != last && IS_DIGIT(*first); ++first)
	{
		result = 10 * result + (*first - '0');
	}

	// fraction part
	if (first != last && *first == '.')
	{
		++first;

		float inv_base = 0.1f;
		for (; first != last && IS_DIGIT(*first); ++first)
		{
			result += (*first - '0') * inv_base;
			inv_base *= 0.1f;
		}
	}

	// result w\o exponent
	result *= sign;

	// exponent
	bool exponent_negative = false;
	int exponent = 0;
	if (first != last && (*first == 'e' || *first == 'E'))
	{
		++first;

		if (*first == '-')
		{
			exponent_negative = true;
			++first;
		}
		else if (*first == '+')
		{
			++first;
		}

		for (; first != last && IS_DIGIT(*first); ++first)
		{
			exponent = 10 * exponent + (*first - '0');
		}
	}

	if (exponent)
	{
		float power_of_ten = 10;
		for (; exponent > 1; exponent--)
		{
			power_of_ten *= 10;
		}

		if (exponent_negative)
		{
			result /= power_of_ten;
		}
		else
		{
			result *= power_of_ten;
		}
	}

	*out = result;

	return first;
}

json_value *json_alloc(block_allocator *allocator)
{
	json_value *value = (json_value *)allocator->malloc(sizeof(json_value));
	memset(value, 0, sizeof(json_value));
	return value;
}

void json_append(json_value *lhs, json_value *rhs)
{
	rhs->parent = lhs;
	if (lhs->last_child)
	{
		lhs->last_child = lhs->last_child->next_sibling = rhs;
	}
	else
	{
		lhs->first_child = lhs->last_child = rhs;
	}
}

#define ERROR(it, desc)\
	*error_pos = it;\
	*error_desc = (char *)desc;\
	*error_line = 1 - escaped_newlines;\
	for (char *c = it; c != source; --c)\
		if (*c == '\n') ++*error_line;\
	return 0

#define CHECK_TOP() if (!top) {ERROR(it, "Unexpected character");}

json_value *json_parse(char *source, char **error_pos, char **error_desc, int *error_line, block_allocator *allocator)
{
	json_value *root = 0;
	json_value *top = 0;

	char *name = 0;
	char *it = source;

	int escaped_newlines = 0;

	while (*it)
	{
		switch (*it)
		{
		case '{':
		case '[':
			{
				// create new value
				json_value *object = json_alloc(allocator);

				// name
				object->name = name;
				name = 0;

				// type
				object->type = (*it == '{') ? JSON_OBJECT : JSON_ARRAY;

				// skip open character
				++it;

				// set top and root
				if (top)
				{
					json_append(top, object);
				}
				else if (!root)
				{
					root = object;
				}
				else
				{
					ERROR(it, "Second root. Only one root allowed");
				}
				top = object;
			}
			break;

		case '}':
		case ']':
			{
				if (!top || top->type != ((*it == '}') ? JSON_OBJECT : JSON_ARRAY))
				{
					ERROR(it, "Mismatch closing brace/bracket");
				}

				// skip close character
				++it;

				// set top
				top = top->parent;
			}
			break;

		case ':':
			if (!top || top->type != JSON_OBJECT)
			{
				ERROR(it, "Unexpected character");
			}
			++it;
			break;

		case ',':
			CHECK_TOP();
			++it;
			break;

		case '"':
			{
				CHECK_TOP();

				// skip '"' character
				++it;

				char *first = it;
				char *last = it;
				while (*it)
				{
					if ((unsigned char)*it < '\x20')
					{
						ERROR(first, "Control characters not allowed in strings");
					}
					else if (*it == '\\')
					{
						switch (it[1])
						{
						case '"':
							*last = '"';
							break;
						case '\\':
							*last = '\\';
							break;
						case '/':
							*last = '/';
							break;
						case 'b':
							*last = '\b';
							break;
						case 'f':
							*last = '\f';
							break;
						case 'n':
							*last = '\n';
							++escaped_newlines;
							break;
						case 'r':
							*last = '\r';
							break;
						case 't':
							*last = '\t';
							break;
						case 'u':
							{
								unsigned int codepoint;
								if (hatoui(it + 2, it + 6, &codepoint) != it + 6)
								{
									ERROR(it, "Bad unicode codepoint");
								}

								if (codepoint <= 0x7F)
								{
									*last = (char)codepoint;
								}
								else if (codepoint <= 0x7FF)
								{
									*last++ = (char)(0xC0 | (codepoint >> 6));
									*last = (char)(0x80 | (codepoint & 0x3F));
								}
								else if (codepoint <= 0xFFFF)
								{
									*last++ = (char)(0xE0 | (codepoint >> 12));
									*last++ = (char)(0x80 | ((codepoint >> 6) & 0x3F));
									*last = (char)(0x80 | (codepoint & 0x3F));
								}
							}
							it += 4;
							break;
						default:
							ERROR(first, "Unrecognized escape sequence");
						}

						++last;
						it += 2;
					}
					else if (*it == '"')
					{
						*last = 0;
						++it;
						break;
					}
					else
					{
						*last++ = *it++;
					}
				}

				if (!name && top->type == JSON_OBJECT)
				{
					// field name in object
					name = first;
				}
				else
				{
					// new string value
					json_value *object = json_alloc(allocator);

					object->name = name;
					name = 0;

					object->type = JSON_STRING;
					object->string_value = first;

					json_append(top, object);
				}
			}
			break;

		case 'n':
		case 't':
		case 'f':
			{
				CHECK_TOP();

				// new null/bool value
				json_value *object = json_alloc(allocator);

				object->name = name;
				name = 0;

				// null
				if (it[0] == 'n' && it[1] == 'u' && it[2] == 'l' && it[3] == 'l')
				{
					object->type = JSON_NULL;
					it += 4;
				}
				// true
				else if (it[0] == 't' && it[1] == 'r' && it[2] == 'u' && it[3] == 'e')
				{
					object->type = JSON_BOOL;
					object->int_value = 1;
					it += 4;
				}
				// false
				else if (it[0] == 'f' && it[1] == 'a' && it[2] == 'l' && it[3] == 's' && it[4] == 'e')
				{
					object->type = JSON_BOOL;
					object->int_value = 0;
					it += 5;
				}
				else
				{
					ERROR(it, "Unknown identifier");
				}

				json_append(top, object);
			}
			break;

		case '-':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			{
				CHECK_TOP();

				// new number value
				json_value *object = json_alloc(allocator);

				object->name = name;
				name = 0;

				object->type = JSON_INT;

				char *first = it;
				while (*it != '\x20' && *it != '\x9' && *it != '\xD' && *it != '\xA' && *it != ',' && *it != ']' && *it != '}')
				{
					if (*it == '.' || *it == 'e' || *it == 'E')
					{
						object->type = JSON_FLOAT;
					}
					++it;
				}

				if (object->type == JSON_INT && atoi(first, it, &object->int_value) != it)
				{
					ERROR(first, "Bad integer number");
				}

				if (object->type == JSON_FLOAT && atof(first, it, &object->float_value) != it)
				{
					ERROR(first, "Bad float number");
				}

				json_append(top, object);
			}
			break;

		default:
			ERROR(it, "Unexpected character");
		}

		// skip white space
		while (*it == '\x20' || *it == '\x9' || *it == '\xD' || *it == '\xA')
		{
			++it;
		}
	}

	if (top)
	{
		ERROR(it, "Not all objects/arrays have been properly closed");
	}

	return root;
}
