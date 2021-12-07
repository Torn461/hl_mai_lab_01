#include "person.h"
#include "database.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <sstream>
#include <fstream>
#include <algorithm>
#include <exception>
#include <future>

using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{

    void Person::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();
            Statement drop_stmt(session);
            
            int max_shard = database::Database::get_max_shard();
            for (int i=0; i < max_shard; i++)
            {
                std::string sharding_hint = " -- sharding:" + std::to_string(i);
                //drop_stmt << "DROP TABLE IF EXISTS Person" + sharding_hint;
                //drop_stmt.execute();

                // (re)create table
                Statement create_stmt(session);
                std::string create_str = "CREATE TABLE IF NOT EXISTS `Person` ( `login` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL, `first_name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL, `last_name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL, `age` INTEGER NOT NULL, PRIMARY KEY (`login`),KEY `fn` (`first_name`),KEY `ln` (`last_name`))";
                create_str += sharding_hint;
                create_stmt << create_str;
                create_stmt.execute();
                std::cout << "table created" + sharding_hint << std::endl;
                Poco::Data::Statement truncate_stmt(session);
                truncate_stmt << "TRUNCATE TABLE `Person`" + sharding_hint;
                truncate_stmt.execute();
            }

            // https://www.onlinedatagenerator.com/
            std::string json;
            std::ifstream is("./database/data.json");
            std::istream_iterator<char> eos;
            std::istream_iterator<char> iit(is);
            while (iit != eos)
                json.push_back(*(iit++));
            is.close();

            Poco::JSON::Parser parser;
            Poco::Dynamic::Var result = parser.parse(json);
            Poco::JSON::Array::Ptr arr = result.extract<Poco::JSON::Array::Ptr>();

            size_t i{0};
            for (i = 0; i < arr->size(); ++i)
            {
                Poco::JSON::Object::Ptr object = arr->getObject(i);
                std::string login = object->getValue<std::string>("login");
                std::string first_name = object->getValue<std::string>("first_name");
                std::string last_name = object->getValue<std::string>("last_name");
                int age = object->getValue<int>("age");
                std::string sharding_hint = database::Database::sharding_hint(login);
                Poco::Data::Statement insert(session);
                std::string insert_str = "INSERT INTO Person (login,first_name,last_name,age) VALUES(?, ?, ?, ?)";
                insert_str += sharding_hint;            
                insert << insert_str,
                    Poco::Data::Keywords::use(login),
                    Poco::Data::Keywords::use(first_name),
                    Poco::Data::Keywords::use(last_name),
                    Poco::Data::Keywords::use(age);
                std::cout << insert_str << std::endl;
                insert.execute();
            }
            std::cout << "Inserted " << i << " records" << std::endl; 
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    Poco::JSON::Object::Ptr Person::toJSON() const
    {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("login", _login);
        root->set("first_name", _first_name);
        root->set("last_name", _last_name);
        root->set("age", _age);

        return root;
    }

    Person Person::fromJSON(const std::string &str)
    {
        Person person;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        person.login() = object->getValue<std::string>("login");
        person.first_name() = object->getValue<std::string>("first_name");
        person.last_name() = object->getValue<std::string>("last_name");
        person.age() = object->getValue<int>("age");

        return person;
    }

    Person Person::read_by_login(std::string login)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            Person p;
            std::string sharding_hint = database::Database::sharding_hint(login);
            std::string select_str = "SELECT login, first_name, last_name, age FROM Person where login=?";
            select_str += sharding_hint;
            select << select_str,
                into(p._login),
                into(p._first_name),
                into(p._last_name),
                into(p._age),
                use(login),
                range(0, 1); //  iterate over result set one row at a time
            std::cout << select_str << std::endl;
            select.execute();
            Poco::Data::RecordSet rs(select);
            if (!rs.moveFirst()) throw std::logic_error("not found");

            return p;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    std::vector<Person> Person::search(std::string first_name, std::string last_name)
    {
        std::vector<Person> result;
        first_name+="%";
        last_name+="%";
        std::vector<std::string> hints = database::Database::get_all_hints();
        std::vector<std::future<std::vector<Person>>> futures;
        // map phase in parallel
        for (const std::string &hint : hints)
        {
            auto handle = std::async(std::launch::async, [first_name, last_name, hint]() -> std::vector<Person>
                            {
                                try
                                {                                    
                                    Person p;
                                    std::vector<Person> result;
                                    Poco::Data::Session session = database::Database::get().create_session();
                                    Statement select(session);
                                    std::string select_str = "SELECT login, first_name, last_name, age FROM Person where first_name LIKE \"" + 
                                                first_name + "\" and last_name LIKE \"" + last_name + "\" ";
                                    select_str += hint;
                                    select << select_str,
                                        into(p._login),
                                        into(p._first_name),
                                        into(p._last_name),
                                        into(p._age),
                                        range(0, 1); //  iterate over result set one row at a time
                                    std::cout << select_str << std::endl;
                                    select.execute();
                                    Poco::Data::RecordSet record_set(select);
                                    if (record_set.moveFirst()) {
                                        std::cout << "result " + hint + " add " + p.get_login() + " " + p.get_first_name() + " " + p.get_last_name() + " " + std::to_string(p.get_age()) << std::endl;
                                        result.push_back(p);
                                    }
                                    while (!select.done())
                                    {
                                        select.execute();
                                        std::cout << "result " + hint + " add " + p.get_login() + " " + p.get_first_name() + " " + p.get_last_name() + " " + std::to_string(p.get_age()) << std::endl;
                                        result.push_back(p);
                                    }
                                    return result;
                                }                                
                                catch (Poco::Data::MySQL::ConnectionException &e)
                                {
                                    std::cout << "connection:" << e.what() << std::endl;
                                    throw;
                                }
                                catch (Poco::Data::MySQL::StatementException &e)
                                {

                                    std::cout << "statement:" << e.what() << std::endl;
                                    throw;
                                }
                            });
                futures.emplace_back(std::move(handle));
        }

        // reduce phase
        // get values
        for(std::future<std::vector<Person>>& res : futures){
            std::vector<Person> v= res.get();
            std::copy(std::begin(v),
                    std::end(v),
                    std::back_inserter(result));
        }
        return result;
    }
   
    void Person::save_to_mysql()
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);
            std::string sharding_hint = database::Database::sharding_hint(_login);
            std::string insert_str = "INSERT INTO Person (login,first_name,last_name,age) VALUES(?, ?, ?, ?)";
            insert_str += sharding_hint;
            insert << insert_str,
                use(_login),
                use(_first_name),
                use(_last_name),
                use(_age);
            std::cout << insert_str << std::endl;
            insert.execute();
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    const std::string &Person::get_login() const
    {
        return _login;
    }

    const std::string &Person::get_first_name() const
    {
        return _first_name;
    }

    const std::string &Person::get_last_name() const
    {
        return _last_name;
    }

    const int &Person::get_age() const
    {
        return _age;
    }

    std::string &Person::login()
    {
        return _login;
    }

    std::string &Person::first_name()
    {
        return _first_name;
    }

    std::string &Person::last_name()
    {
        return _last_name;
    }

    int &Person::age()
    {
        return _age;
    }
}