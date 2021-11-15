#ifndef PERSONHANDLER_H
#define PERSONHANDLER_H

#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Exception.h"
#include "Poco/ThreadPool.h"
#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include <iostream>
#include <iostream>
#include <fstream>
#include <regex>

using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::ThreadPool;
using Poco::Timestamp;
using Poco::Net::HTMLForm;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPRequestHandlerFactory;
using Poco::Net::HTTPServer;
using Poco::Net::HTTPServerParams;
using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPServerResponse;
using Poco::Net::NameValueCollection;
using Poco::Net::ServerSocket;
using Poco::Util::Application;
using Poco::Util::HelpFormatter;
using Poco::Util::Option;
using Poco::Util::OptionCallback;
using Poco::Util::OptionSet;
using Poco::Util::ServerApplication;

#include "../../database/person.h"

class PersonHandler : public HTTPRequestHandler
{
private:
    bool check_login(const std::string &login, std::string &reason)
    {        
        if (login.length() < 3)
        {
            reason = "Login must be at least 3 signs";
            return false;
        }

        if (login.find(' ') != std::string::npos)
        {
            reason = "Login can't contain spaces";
            return false;
        }

        if (login.find('\t') != std::string::npos)
        {
            reason = "Login can't contain spaces";
            return false;
        }

        return true;
    };

    bool check_name(const std::string &name, std::string &reason)
    {
        if (name.length() < 3)
        {
            reason = "Name must be at leas 3 signs";
            return false;
        }

        if (name.find(' ') != std::string::npos)
        {
            reason = "Name can't contain spaces";
            return false;
        }

        if (name.find('\t') != std::string::npos)
        {
            reason = "Name can't contain spaces";
            return false;
        }

        return true;
    };

    bool check_age(const std::string &age, std::string &reason)
    {        
        if (!std::regex_match(age, std::regex("[1-9][0-9]*")))
        {
            reason = "Age (" + age + ") must be a positive number";
            return false;
        }

        return true;
    };

public:
    PersonHandler(const std::string &format) : _format(format)
    {
    }

    void handleRequest(HTTPServerRequest &request,
                       HTTPServerResponse &response)
    {
        HTMLForm form(request, request.stream());
        response.setChunkedTransferEncoding(true);
        response.setContentType("application/json");
        std::ostream &ostr = response.send();

        if (request.getMethod() == "GET") 
        {
            if (form.has("login"))
            {
                std::string login = form.get("login");
                bool no_cache = false;
                if (form.has("no_cache"))
                    no_cache = true;
                // read from cache
                // Шаблон «сквозное чтение»
                // если записи нет в кеше - ситаем из БД
                if (!no_cache)
                {
                    try
                    {
                        database::Person result = database::Person::read_from_cache_by_login(login);
                        std::cout << "item from cache: " << login << std::endl;
                        Poco::JSON::Stringifier::stringify(result.toJSON(), ostr);
                        return;
                    }
                    catch (...)
                    {
                        std::cout << "cache missed for login: " << login << std::endl;
                    }
                }
                try
                {
                    database::Person result = database::Person::read_by_login(login);
                    if (!no_cache)
                        result.save_to_cache();
                    Poco::JSON::Stringifier::stringify(result.toJSON(), ostr);
                    return;
                }
                catch (...)
                {
                    ostr << "{ \"result\": false , \"reason\": \"not found\" }";
                    return;
                }
            }
            else if (form.has("first_name") || form.has("last_name"))
            {
                try
                {
                    std::string fn = form.get("first_name");
                    std::string ln = form.get("last_name");
                    auto results = database::Person::search(fn,ln);
                    Poco::JSON::Array arr;
                    for (auto s : results)
                        arr.add(s.toJSON());
                    Poco::JSON::Stringifier::stringify(arr, ostr);
                }
                catch (...)
                {
                    ostr << "{ \"result\": false , \"reason\": \"not gound\" }";
                    return;
                }
                return;
            }
        }
        else if (request.getMethod() == "POST") 
        {
            if (form.has("login") && form.has("first_name") && form.has("last_name") && form.has("age"))
            {
                database::Person person;
                person.login() = form.get("login");
                person.first_name() = form.get("first_name");
                person.last_name() = form.get("last_name");

                bool check_result = true;
                std::string message;
                std::string reason;

                if (!check_login(person.get_login(), reason))
                {
                    check_result = false;
                    message += reason;
                    message += "<br>";
                }
                try
                {
                    database::Person result = database::Person::read_by_login(person.login());
                    ostr << "{ \"result\": false , \"reason\": \"Login " + person.login() + " already exists.\" }";
                    return;
                }
                catch (...)
                {
                }

                if (!check_name(person.get_first_name(), reason))
                {
                    check_result = false;
                    message += reason;
                    message += "<br>";
                }

                if (!check_name(person.get_last_name(), reason))
                {
                    check_result = false;
                    message += reason;
                    message += "<br>";
                }

                if (!check_age(form.get("age"), reason))
                {
                    check_result = false;
                    message += reason;
                    message += "<br>";
                } 
                else 
                {
                    person.age() = atoi(form.get("age").c_str());
                }

                if (check_result)
                {
                    try
                    {
                        // Шаблон «сквозная-запись»
                        // пишем и в БД и в кеш
                        person.save_to_mysql();
                        person.save_to_cache();
                        ostr << "{ \"result\": true }";
                        return;
                    }
                    catch (...)
                    {
                        ostr << "{ \"result\": false , \"reason\": \" database error\" }";
                        return;
                    }
                }
                else
                {
                    ostr << "{ \"result\": false , \"reason\": \"" << message << "\" }";
                    return;
                }
            }
        }
    }

private:
    std::string _format;
};
#endif // !PERSONHANDLER_H