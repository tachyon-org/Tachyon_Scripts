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

    def compute_timestamp_range(self):
        max_starting_timestamp = 0
        min_ending_timestamp = 2000000000

        for field in self.fields:
            first_timestamp_found = False

            with open("{}/{}.xml".format(self.data_dir_path, field)) as xml_file_obj:
                xml_content = xml_file_obj.readlines()

                for index in range(len(xml_content)):
                    if index>0 and "<database>" in xml_content[index-1] and not first_timestamp_found:
                        line = xml_content[index]
                        epoch_timestamp = int(line.split("/")[1].split("-->")[0].strip())
                        max_starting_timestamp = max(max_starting_timestamp, epoch_timestamp)
                        first_timestamp_found = True

                    if index<len(xml_content)-1 and  "</database>" in xml_content[index+1]:
                        line = xml_content[index]
                        epoch_timestamp = int(line.split("/")[1].split("-->")[0].strip())
                        min_ending_timestamp = min(min_ending_timestamp, epoch_timestamp)
                        break

        self.max_starting_timestamp = max_starting_timestamp
        self.min_ending_timestamp = min_ending_timestamp

    def generate_csv_file(self):
        self.compute_timestamp_range()
        array = [[ts] for ts in range(self.max_starting_timestamp, self.min_ending_timestamp+1, 15)]

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
                        if epoch_timestamp>=self.max_starting_timestamp and epoch_timestamp<=self.min_ending_timestamp:
                            value = line.split("<v>")[1].split("</v>")[0]
                            array[(epoch_timestamp-self.max_starting_timestamp)//15].append(value)

        header = ["Epoch Timestamp"]
        header.extend(self.fields)
        array.insert(0, header)
        numpy.savetxt(self.csv_file_path, array, fmt='%s', delimiter=",")

        self.teardownXMLs()

if __name__=="__main__":
    if(len(sys.argv)) != 2:
        print("{} takes one argument: conf_file_path".format(sys.argv[0]))
        sys.exit(1)
    rrd_to_csv_converter = RRDtoCSV(sys.argv[1])
    rrd_to_csv_converter.generate_csv_file()