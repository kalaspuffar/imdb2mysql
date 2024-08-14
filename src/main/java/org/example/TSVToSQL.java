package org.example;

import java.io.*;
import java.util.zip.GZIPOutputStream;

public class TSVToSQL {

    public static int parseID(String id) {
        return Integer.parseInt(id.substring(2));
    }

    public static void processTitle(BufferedWriter wr) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("movie/title.basics.tsv"));

        wr.write("SET autocommit=0;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS titles;");
        wr.newLine();

        wr.write(
        "CREATE TABLE titles (id int NOT NULL, titleType varchar(20) NOT NULL, primaryTitle text NOT NULL, " +
            "originalTitle text, isAdult boolean, startYear int, endYear int, runtimeMinutes int, " +
            "genres varchar(255));"
        );
        wr.newLine();
        wr.newLine();

        boolean first = true;

        String line;
        while ((line = br.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            String[] lineArr = line.split("\t");

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO titles (id, titleType, primaryTitle, originalTitle, ");
            sb.append("isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (");
            sb.append(parseID(lineArr[0]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[1]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[2]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[3]));
            sb.append(", ");
            sb.append(lineArr[4].replace("\\N", "NULL"));
            sb.append(", ");
            sb.append(lineArr[5].replace("\\N", "NULL"));
            sb.append(", ");
            sb.append(lineArr[6].replace("\\N", "NULL"));
            sb.append(", ");
            sb.append(lineArr[7].replace("\\N", "NULL"));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[8]));
            sb.append(");");

            wr.write(sb.toString());
            wr.newLine();
        }

        wr.write("COMMIT;");
        wr.newLine();
        wr.write("SET autocommit=1;");
        wr.newLine();

        wr.write("ALTER TABLE titles ADD PRIMARY KEY (id);");
        wr.newLine();

    }

    public static void processTitleLanguage(BufferedWriter wr) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("movie/title.akas.tsv"));

        wr.write("SET autocommit=0;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS titles_lang;");
        wr.newLine();

        wr.write(
        "CREATE TABLE titles_lang (title_id int NOT NULL, ordering int, title text NOT NULL, " +
            "region varchar(5), language varchar(5), types varchar(40), " +
            "attributes varchar(255), isOriginalTitle boolean);"
        );
        wr.newLine();
        wr.newLine();

        boolean first = true;

        String line;
        while ((line = br.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            String[] lineArr = line.split("\t");

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO titles_lang (title_id, ordering, title, region, ");
            sb.append("language, types, attributes, isOriginalTitle) VALUES (");
            sb.append(parseID(lineArr[0]));
            sb.append(", ");
            sb.append(lineArr[1]);
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[2]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[3]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[4]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[5]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[6]));
            sb.append(", ");
            sb.append(lineArr[7]);
            sb.append(");");

            wr.write(sb.toString());
            wr.newLine();
        }

        wr.write("COMMIT;");
        wr.newLine();
        wr.write("SET autocommit=1;");
        wr.newLine();

        wr.write("ALTER TABLE titles_lang ADD PRIMARY KEY (title_id, ordering);");
        wr.newLine();
        wr.write("ALTER TABLE titles_lang ADD CONSTRAINT fk_titles_lang_title_id FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();
    }

    public static void processNames(BufferedWriter wr) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("movie/name.basics.tsv"));

        wr.write("SET autocommit=0;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS names;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS knownfor;");
        wr.newLine();

        wr.write(
        "CREATE TABLE names (id int NOT NULL, primaryName varchar(255), " +
            "birthYear int, deathYear int, primaryProfession varchar(255));"
        );
        wr.newLine();
        wr.write(
            "CREATE TABLE knownfor (name_id int NOT NULL, title_id int NOT NULL);"
        );
        wr.newLine();
        wr.newLine();

        boolean first = true;

        String line;
        while ((line = br.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            String[] lineArr = line.split("\t");

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO names (id, primaryName, birthYear, deathYear, ");
            sb.append("primaryProfession) VALUES (");
            int nameId = parseID(lineArr[0]);
            sb.append(nameId);
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[1]));
            sb.append(", ");
            sb.append(lineArr[2].replace("\\N", "NULL"));
            sb.append(", ");
            sb.append(lineArr[3].replace("\\N", "NULL"));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[4]));
            sb.append(");");
            wr.write(sb.toString());
            wr.newLine();

            if (!lineArr[5].equals("\\N")) {
                for (String s : lineArr[5].split(",")) {
                    sb = new StringBuilder();
                    sb.append("INSERT INTO knownfor (title_id, name_id) VALUES (");
                    int titleId = parseID(s);
                    sb.append(titleId);
                    sb.append(", ");
                    sb.append(nameId);
                    sb.append(");");
                    wr.write(sb.toString());
                    wr.newLine();
                }
            }
        }

        wr.write("COMMIT;");
        wr.newLine();
        wr.write("SET autocommit=1;");
        wr.newLine();

        wr.write("ALTER TABLE names ADD PRIMARY KEY (id);");
        wr.newLine();

        wr.write("ALTER TABLE knownfor ADD PRIMARY KEY (name_id, title_id);");
        wr.newLine();
        wr.write("ALTER TABLE knownfor ADD CONSTRAINT fk_knownfor_name_id FOREIGN KEY (name_id) REFERENCES names(id) ON DELETE CASCADE;");
        wr.newLine();
        wr.write("ALTER TABLE knownfor ADD CONSTRAINT fk_knownfor_title_id FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();
    }

    public static void processCrew(BufferedWriter wr) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("movie/title.crew.tsv"));

        wr.write("SET autocommit=0;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS directors;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS writers;");
        wr.newLine();

        wr.write("CREATE TABLE directors (name_id int NOT NULL, title_id int NOT NULL);");
        wr.newLine();
        wr.write("CREATE TABLE writers (name_id int NOT NULL, title_id int NOT NULL);");
        wr.newLine();
        wr.newLine();

        boolean first = true;

        String line;
        while ((line = br.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            String[] lineArr = line.split("\t");

            StringBuilder sb = new StringBuilder();
            int titleId = parseID(lineArr[0]);

            for (String s : lineArr[1].split(",")) {
                if (s.length() < 3) continue;
                int nameId = parseID(s);
                sb = new StringBuilder();
                sb.append("INSERT INTO directors (title_id, name_id) VALUES (");
                sb.append(titleId);
                sb.append(", ");
                sb.append(nameId);
                sb.append(");");
                wr.write(sb.toString());
                wr.newLine();
            }
            for (String s : lineArr[2].split(",")) {
                if (s.length() < 3) continue;
                sb = new StringBuilder();
                sb.append("INSERT INTO writers (title_id, name_id) VALUES (");
                int nameId = parseID(s);
                sb.append(titleId);
                sb.append(", ");
                sb.append(nameId);
                sb.append(");");
                wr.write(sb.toString());
                wr.newLine();
            }
        }

        wr.write("COMMIT;");
        wr.newLine();
        wr.write("SET autocommit=1;");
        wr.newLine();

        wr.write("ALTER TABLE directors ADD PRIMARY KEY (name_id, title_id);");
        wr.newLine();
        wr.write("ALTER TABLE directors ADD CONSTRAINT fk_directors_name_id FOREIGN KEY (name_id) REFERENCES names(id) ON DELETE CASCADE;");
        wr.newLine();
        wr.write("ALTER TABLE directors ADD CONSTRAINT fk_directors_title_id FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();

        wr.write("ALTER TABLE writers ADD PRIMARY KEY (name_id, title_id);");
        wr.newLine();
        wr.write("ALTER TABLE writers ADD CONSTRAINT fk_writers_name_id FOREIGN KEY (name_id) REFERENCES names(id) ON DELETE CASCADE;");
        wr.newLine();
        wr.write("ALTER TABLE writers ADD CONSTRAINT fk_writers_title_id FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();
    }

    public static void processEpisodes(BufferedWriter wr) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("movie/title.episode.tsv"));

        wr.write("SET autocommit=0;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS episodes;");
        wr.newLine();

        wr.write(
        "CREATE TABLE episodes (id int NOT NULL, parentId int NOT NULL, seasonNumber int, episodeNumber int);"
        );
        wr.newLine();
        wr.newLine();

        boolean first = true;

        String line;
        while ((line = br.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            String[] lineArr = line.split("\t");

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO episodes (id, parentId, seasonNumber, episodeNumber) VALUES (");
            sb.append(parseID(lineArr[0]));
            sb.append(", ");
            sb.append(parseID(lineArr[1]));
            sb.append(", ");
            sb.append(lineArr[2].replace("\\N", "NULL"));
            sb.append(", ");
            sb.append(lineArr[3].replace("\\N", "NULL"));
            sb.append(");");

            wr.write(sb.toString());
            wr.newLine();
        }

        wr.write("COMMIT;");
        wr.newLine();
        wr.write("SET autocommit=1;");
        wr.newLine();

        wr.write("ALTER TABLE episodes ADD PRIMARY KEY (id);");
        wr.newLine();
        wr.write("ALTER TABLE episodes ADD CONSTRAINT fk_episodes_id FOREIGN KEY (id) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();
        wr.write("ALTER TABLE episodes ADD CONSTRAINT fk_episodes_parentId FOREIGN KEY (parentId) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();
    }

    public static String wrapOrNull(String s) {
        if (s.equals("\\N")) {
            return "NULL";
        } else {
            return "\"" + s.replace("\"", "'") + "\"";
        }
    }

    public static void processPrincipals(BufferedWriter wr) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("movie/title.principals.tsv"));

        wr.write("SET autocommit=0;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS principals;");
        wr.newLine();

        wr.write(
        "CREATE TABLE principals (title_id int NOT NULL, ordering int, name_id int NOT NULL, " +
            "category varchar(255), job text, characters text);"
        );
        wr.newLine();
        wr.newLine();

        boolean first = true;

        String line;
        while ((line = br.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            String[] lineArr = line.split("\t");

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO principals (title_id, ordering, name_id, category, ");
            sb.append("job, characters) VALUES (");
            sb.append(parseID(lineArr[0]));
            sb.append(", ");
            sb.append(lineArr[1]);
            sb.append(", ");
            sb.append(parseID(lineArr[2]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[3]));
            sb.append(", ");
            sb.append(wrapOrNull(lineArr[4]));
            sb.append(", ");
            String character = lineArr[5];
            character = character.replace("[", "");
            character = character.replace("\\", "");
            character = character.replace("\"", "");
            character = character.replace("]", "");
            sb.append(wrapOrNull(character));
            sb.append(");");

            wr.write(sb.toString());
            wr.newLine();
        }

        wr.write("COMMIT;");
        wr.newLine();
        wr.write("SET autocommit=1;");
        wr.newLine();

        wr.write("ALTER TABLE principals ADD PRIMARY KEY (title_id, ordering);");
        wr.newLine();
        wr.write("ALTER TABLE principals ADD CONSTRAINT fk_principals_title_id FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();
        wr.write("ALTER TABLE principals ADD CONSTRAINT fk_principals_name_id FOREIGN KEY (name_id) REFERENCES names(id) ON DELETE CASCADE;");
        wr.newLine();
    }

    public static void processRatings(BufferedWriter wr) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("movie/title.ratings.tsv"));

        wr.write("SET autocommit=0;");
        wr.newLine();
        wr.write("DROP TABLE IF EXISTS ratings;");
        wr.newLine();

        wr.write(
            "CREATE TABLE ratings (title_id int NOT NULL, averageRating float NOT NULL, numVotes int);"
        );
        wr.newLine();
        wr.newLine();

        boolean first = true;

        String line;
        while ((line = br.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            String[] lineArr = line.split("\t");

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ratings (title_id, averageRating, numVotes) VALUES (");
            sb.append(parseID(lineArr[0]));
            sb.append(", ");
            sb.append(lineArr[1]);
            sb.append(", ");
            sb.append(lineArr[2]);
            sb.append(");");

            wr.write(sb.toString());
            wr.newLine();
        }

        wr.write("COMMIT;");
        wr.newLine();
        wr.write("SET autocommit=1;");
        wr.newLine();

        wr.write("ALTER TABLE ratings ADD PRIMARY KEY (title_id);");
        wr.newLine();
        wr.write("ALTER TABLE ratings ADD CONSTRAINT fk_ratings_title_id FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE;");
        wr.newLine();
    }

    public static void main(String[] args) throws Exception {
        FileOutputStream output = new FileOutputStream("movie/data.sql.gz");
        BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8"));

        wr.write("SET NAMES 'utf8mb4';");
        wr.newLine();

        System.out.println("Write titles");
        processTitle(wr);
        System.out.println("Add translations");
        processTitleLanguage(wr);
        System.out.println("Write names table");
        processNames(wr);
        System.out.println("Add crew links");
        processCrew(wr);
        System.out.println("Link all episodes");
        processEpisodes(wr);
        System.out.println("Add principals, who did what");
        processPrincipals(wr);
        System.out.println("Create table for ratings");
        processRatings(wr);
        System.out.println("DONE!");
        wr.flush();
        wr.close();
    }
}
