import yaml
import os
import sys
import numpy

class RRDtoCSV:
    def __init__(self, conf_file_path):
        with open(conf_file_path, "r") as conf_file_obj:
            conf_file = yaml.safe_load(conf_file_obj)

        self.data_dir_path = conf_file["data_dir_path"]
        self.csv_file_path = conf_file["csv_file_path"]
        if "all_fields" not in conf_file["fields"]:
            self.fields = conf_file["fields"]
        else:
            self.fields = [file_name.split(".")[0] for file_name in os.listdir(self.data_dir_path) if "rrd" in file_name]

        self.generateXMLs()

    def generateXMLs(self):
        for field in self.fields:
            os.system("rrdtool dump {0}/\'{1}.rrd\' {0}/\'{1}.xml\'".format(self.data_dir_path, field))

    def teardownXMLs(self):
        for field in self.fields:
            os.remove("{0}/{1}.xml".format(self.data_dir_path, field))

    def generate_csv_file(self):
        timestamp_dic = {}

        for field in self.fields:
            traversing_database = False
            with open("{}/{}.xml".format(self.data_dir_path, field)) as xml_file_obj:
                xml_content = xml_file_obj.readlines()

                for line in xml_content:
                    if "<database>" in line:
                        traversing_database = True
                        continue
                    if "</database>" in line:
                        traversing_database = False
                        break
                    if traversing_database:
                        epoch_timestamp = int(line.split("/")[1].split("-->")[0].strip())

                        if epoch_timestamp not in timestamp_dic:
                            timestamp_dic[epoch_timestamp] = ["NaN" for _ in range(len(self.fields))]
                        value = line.split("<v>")[1].split("</v>")[0]
                        timestamp_dic[epoch_timestamp][self.fields.index(field)] = value

        matrix_format_data = []
        header = ["Epoch Timestamp"]
        header.extend(self.fields)
        matrix_format_data.append(header)

        for ts in timestamp_dic:
            matrix_format_data.append([ts]+timestamp_dic[ts])

        numpy.savetxt(self.csv_file_path, matrix_format_data, fmt='%s', delimiter=",")

        self.teardownXMLs()

if __name__=="__main__":
    if(len(sys.argv)) != 2:
        print("{} takes one argument: conf_file_path".format(sys.argv[0]))
        sys.exit(1)
    rrd_to_csv_converter = RRDtoCSV(sys.argv[1])
    rrd_to_csv_converter.generate_csv_file()