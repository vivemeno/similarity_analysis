from textFileProcessor import TextFileProcessor;


class InitialSetup:

    def create_initial_tables(self):
        queryList = ["CREATE TABLE users (userId text PRIMARY KEY)",
                     "CREATE TABLE images (imageId text PRIMARY KEY)",
                     "CREATE TABLE locations (locationId text PRIMARY KEY, locationName text)",
                     "CREATE TABLE terms (term text PRIMARY KEY)",
                     "CREATE TABLE termsPerUser (term text NOT NULL, "
                     "userId text NOT NULL, TF integer, DF integer, TF_IDF REAL)",
                     "CREATE TABLE termsPerImage (term text NOT NULL, "
                     "imageId text NOT NULL, TF integer, DF integer, TF_IDF REAL)",
                     "CREATE TABLE termsPerLocation (term text NOT NULL, "
                     "locationId text NOT NULL, TF integer, DF integer, TF_IDF REAL)"
                     ]
        self._database_operations.executeWriteQueries(queryList)

    def __init__(self, database_operations):
        self._textTermsPerUserFile = "/desctxt/devset_textTermsPerUser.txt"
        self._textTermsPerImageFile = "/desctxt/devset_textTermsPerImage.txt"
        self._textTermsPerPOIwFolderNamesFile = "/desctxt/devset_textTermsPerPOI.wFolderNames.txt"
        self._database_operations = database_operations
        self.uniqueTerms = set();


    def process_text_terms_per_user(self, line):
        # regex = "^[\w]+@[\w]+\s\".+\"\s\d+\s\d+\s\d+\.\d+$"
        # compiledPattern = re.compile(regex);
        quoteIndex = line.find("\"");
        userId = line[:quoteIndex]
        userId = userId.strip();
        insertUserQuery = "INSERT into users(userId) values(\"%s\")" %userId
        term_data = line[quoteIndex:]
        insertTermPerUserQuery = "INSERT into termsPerUser(term, userId, TF, DF, TF_IDF) values(%s, \"%s\", %d, %d, %f)"
        queries = self.process_text_file_generic(term_data, userId, insertTermPerUserQuery)
        queries.append(insertUserQuery)
        self._database_operations.executeWriteQueries(queries)

    def process_text_terms_per_image(self, line):
        # regex = "^[\w]+@[\w]+\s\".+\"\s\d+\s\d+\s\d+\.\d+$"
        # compiledPattern = re.compile(regex);
        quoteIndex = line.find("\"");
        imageId = line[:quoteIndex]
        imageId = imageId.strip();
        insertImageQuery = "INSERT into images(imageId) values(\"%s\")" % imageId
        term_data = line[quoteIndex:]
        insertTermPerImageQuery = "INSERT into termsPerImage(term, imageId, TF, DF, TF_IDF) values(%s, \"%s\", %d, %d, %f)"
        queries = self.process_text_file_generic(term_data, imageId, insertTermPerImageQuery)
        queries.append(insertImageQuery)
        self._database_operations.executeWriteQueries(queries)

    def process_text_terms_per_POI(self, line):
        # regex = "^[\w]+@[\w]+\s\".+\"\s\d+\s\d+\s\d+\.\d+$"
        # compiledPattern = re.compile(regex);
        quoteIndex = line.find("\"");
        location_data = line[:quoteIndex]
        location_data_split = location_data.split(" ", 1)
        location_id = location_data_split[0]
        location_name = location_data_split[1]
        location_name = location_name.strip();
        insertLocationQuery = "INSERT into locations(locationId, locationName) values(\"%s\", \"%s\")" % (location_id, location_name)
        term_data = line[quoteIndex:]
        insertTermPerLocationQuery = "INSERT into termsPerLocation(term, locationId, TF, DF, TF_IDF) values(%s, \"%s\", %d, %d, %f)"
        queries = self.process_text_file_generic(term_data, location_id, insertTermPerLocationQuery)
        queries.append(insertLocationQuery)
        self._database_operations.executeWriteQueries(queries)

    def process_text_file_generic(self, term_data, primary_metric, insert_term_per_metric_query):
        queries = []
        while term_data:
            text_indicators = term_data.split(" ", 4)
            if len(text_indicators) == 5:

                term = text_indicators[0]
                TF = int(text_indicators[1])
                DF = int(text_indicators[2])
                TF_IDF = float(text_indicators[3])
                term_data = text_indicators[4];

                if term not in self.uniqueTerms:
                    self.uniqueTerms.add(term)
                    insertTermQuery = "INSERT into terms(term) values(%s)" % term
                    queries.append(insertTermQuery)

                insertTermPerMetricQuery = insert_term_per_metric_query % (
                term, primary_metric, TF, DF, TF_IDF)

                queries.append(insertTermPerMetricQuery)
            else:
                term_data = None;
        return  queries;

    def process_desctxt_files(self):
        text_processor = TextFileProcessor();
        text_processor.process_text_file(self._textTermsPerUserFile, self.process_text_terms_per_user)
        text_processor.process_text_file(self._textTermsPerImageFile, self.process_text_terms_per_image)
        text_processor.process_text_file(self._textTermsPerPOIwFolderNamesFile, self.process_text_terms_per_POI)

    def setup_database_from_devset_data(self):
        self.create_initial_tables();
        self.process_desctxt_files()
        return None;