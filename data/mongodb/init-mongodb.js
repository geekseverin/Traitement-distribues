// MongoDB initialization script
// This script will create sample data in MongoDB

// Switch to bigdata database
use('bigdata');

// Create employees collection with sample data
db.employees.insertMany([
    {
        id: 1,
        name: "John Doe",
        age: 28,
        department: "IT",
        salary: 45000,
        city: "Paris",
        hire_date: new Date("2022-01-15")
    },
    {
        id: 2,
        name: "Jane Smith",
        age: 32,
        department: "Sales",
        salary: 52000,
        city: "London",
        hire_date: new Date("2021-06-10")
    },
    {
        id: 3,
        name: "Bob Johnson",
        age: 45,
        department: "Finance",
        salary: 68000,
        city: "New York",
        hire_date: new Date("2020-03-22")
    },
    {
        id: 4,
        name: "Alice Brown",
        age: 29,
        department: "HR",
        salary: 43000,
        city: "Berlin",
        hire_date: new Date("2022-09-05")
    },
    {
        id: 5,
        name: "Charlie Wilson",
        age: 38,
        department: "IT",
        salary: 61000,
        city: "Tokyo",
        hire_date: new Date("2021-11-18")
    },
    {
        id: 6,
        name: "Eva Davis",
        age: 34,
        department: "Marketing",
        salary: 55000,
        city: "Sydney",
        hire_date: new Date("2021-08-14")
    },
    {
        id: 7,
        name: "Frank Miller",
        age: 41,
        department: "Sales",
        salary: 59000,
        city: "Toronto",
        hire_date: new Date("2020-12-03")
    },
    {
        id: 8,
        name: "Grace Lee",
        age: 27,
        department: "HR",
        salary: 41000,
        city: "Seoul",
        hire_date: new Date("2023-02-28")
    },
    {
        id: 9,
        name: "Henry Garcia",
        age: 36,
        department: "Finance",
        salary: 57000,
        city: "Madrid",
        hire_date: new Date("2021-04-12")
    },
    {
        id: 10,
        name: "Ivy Wang",
        age: 31,
        department: "IT",
        salary: 48000,
        city: "Beijing",
        hire_date: new Date("2022-07-20")
    }
]);

// Create indexes for better performance
db.employees.createIndex({ department: 1 });
db.employees.createIndex({ salary: 1 });
db.employees.createIndex({ age: 1 });

// Create a collection for results
db.results.createIndex({ timestamp: 1 });

print("MongoDB initialization completed!");
print("Inserted " + db.employees.count() + " employee records.");
print("Available collections: " + db.getCollectionNames());