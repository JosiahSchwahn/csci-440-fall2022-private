package edu.montana.csci.csci440.helpers;

import edu.montana.csci.csci440.model.Employee;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EmployeeHelper {

    public static String makeEmployeeTree() {
        // TODO, change this to use a single query operation to get all employees
        Employee employee = new Employee(); // root employee

        //Map<bosses' ID, employees that report to that boss>
        Map<Long, List<Employee>> employeeMap = new HashMap<>();

        List<Employee> employees = Employee.all();

        for (Employee emp : employees) {
            if (employeeMap.containsKey(emp.getReportTo())) {
                //hashMap already contains bosses ID
                List<Employee> currentReportsTo = employeeMap.get(emp.getReportTo());
                currentReportsTo.add(emp);
                employeeMap.put(emp.getReportTo(), currentReportsTo);
            } else {
                //hashMap doesn't contain bosses ID yet
                if (emp.getReportTo() != 0) {
                    List<Employee> empList = new LinkedList<>();
                    empList.add(emp);
                    employeeMap.put(emp.getReportTo(), empList);
                }
            }
            if (emp.getEmployeeId() == 1) {
                employee = emp; //sets root employee to
            }
        }

        return "<ul>" + makeTree(employee, employeeMap) + "</ul>";
    }

    // TODO - currently this method just uses the employee.getReports() function, which
    //  issues a query.  Change that to use the employeeMap variable instead
    public static String makeTree(Employee employee, Map<Long, List<Employee>> employeeMap) {
        String list = "<li><a href='/employees/" + employee.getEmployeeId() + "'>"
                + employee.getEmail() + "</a><ul>";

        //pulls reportsTo list from hashMap instead of the DB
        List<Employee> reports = employeeMap.get(employee.getEmployeeId());

        if (reports != null) {
            for (Employee report : reports) {
                list += makeTree(report, employeeMap);
            }
        }

        return list + "</ul></li>";
    }
}
