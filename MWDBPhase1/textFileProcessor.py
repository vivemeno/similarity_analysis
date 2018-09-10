class TextFileProcessor:
    def __init__(self):
        self.__basePath = "/home/vivemeno/MWD Project Data/devset/extractedData"

    def yieldLines(self, fileName):
        with open(fileName) as f:
            lines = f.readlines()
            for line in lines:
                yield line;

            f.close();

    def process_text_file(self, fileName, line_processor):
        for line in self.readFile(fileName):
            line_processor(line)


    def readFile(self, fileName):
        fileName = self.__basePath + fileName;
        return self.yieldLines(fileName);
