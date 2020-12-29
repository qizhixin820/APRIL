

if __name__ == '__main__':
    predicates = set()
    rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    ub = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#"
    # 1
    predicates.add(rdf + "type")
    predicates.add(ub + "takesCourse")
    # 2
    predicates.add(rdf + "type")
    predicates.add(ub + "memberOf")
    predicates.add(ub + "subOrganizationOf")
    predicates.add(ub + "undergraduateDegreeFrom")
    # 3
    predicates.add(rdf + "type")
    predicates.add(ub + "publicationAuthor")
    # 4
    predicates.add(rdf + "type")
    predicates.add(ub + "worksFor")
    predicates.add(ub + "name")
    predicates.add(ub + "emailAddress")
    predicates.add(ub + "telephone")
    # 5
    predicates.add(rdf + "type")
    predicates.add(ub + "memberOf")
    # 6
    predicates.add(rdf + "type")
    # 7
    predicates.add(rdf + "type")
    predicates.add(ub + "takesCourse")
    predicates.add(ub + "teacherOf")
    # 8
    predicates.add(rdf + "type")
    predicates.add(ub + "memberOf")
    predicates.add(ub + "subOrganizationOf")
    predicates.add(ub + "emailAddress")
    # 9
    predicates.add(rdf + "type")
    predicates.add(ub + "advisor")
    predicates.add(ub + "teacherOf")
    predicates.add(ub + "takesCourse")
    # 10
    predicates.add(rdf + "type")
    predicates.add(ub + "takesCourse")
    # 11
    predicates.add(rdf + "type")
    predicates.add(ub + "subOrganizationOf")
    # 12
    predicates.add(rdf + "type")
    predicates.add(ub + "worksFor")
    predicates.add(ub + "subOrganizationOf")
    # 13
    predicates.add(rdf + "type")
    predicates.add(ub + "hasAlumnus")
    # 14
    predicates.add(rdf + "type")

    write_file = open("../../data/University_reduce.nt", "w")
    for i in range(10):
        with open("D:\\DriveY\\IntelliJ\\data\\" + "University" + str(i) + ".nt", "r") as f:
            lines = f.readlines()
            for line in lines:
                if line.split(" ")[1][1:-1] in predicates:
                    write_file.write(line)
    write_file.close()