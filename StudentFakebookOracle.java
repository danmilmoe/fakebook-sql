package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    // (B) Find the birth month in which the most users were born
    // (C) Find the birth month in which the fewest users (at least one) were born
    // (D) Find the IDs, first names, and last names of users born in the month
    // identified in (B)
    // (E) Find the IDs, first names, and last name of users born in the month
    // identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find
    // the appropriate
    // mechanisms for opening up a statement, executing a query, walking through
    // results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that
                                                                    // birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month,
                                                                          // descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); // it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); // it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    // (B) The first name(s) with the fewest letters
    // (C) The first name held by the most users
    // (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Find lengths of first names and first names sorted by
            // lengths descending and first names alphabetically

            ResultSet rst = stmt.executeQuery(
                    "SELECT DISTINCT First_Name, LENGTH(First_Name) AS Lengths " +
                            "FROM " + UsersTable + " " +
                            "WHERE LENGTH(First_Name) = (SELECT MAX(LENGTH(First_Name)) FROM " + UsersTable + ") " +
                            "OR " +
                            "LENGTH(First_Name) = (SELECT MIN(LENGTH(First_Name)) FROM " + UsersTable + ") " +
                            "ORDER BY Lengths DESC, First_Name ASC");

            FirstNameInfo info = new FirstNameInfo();
            long max = -1;
            while (rst.next()) {
                if (rst.isFirst()) {
                    max = rst.getLong(2);
                }
                if (max == rst.getLong(2)) {
                    info.addLongName(rst.getString(1));
                } else {
                    info.addShortName(rst.getString(1));
                }
            }

            rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Frequency, First_Name " +
                            "FROM " + UsersTable + " " +
                            "GROUP BY First_Name " +
                            "ORDER BY Frequency DESC, First_Name ASC");

            long freq = -1;
            while (rst.next()) {
                if (rst.getLong(1) >= freq) {
                    freq = rst.getLong(1);
                } else {
                    break;
                }
                info.addCommonName(rst.getString(2));
            }

            info.setCommonNameCount(freq);

            rst.close();
            stmt.close();

            return info; // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any
    // friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only
    // contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                    "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                            "FROM " + UsersTable + " U " +
                            "WHERE U.User_ID NOT IN (" +
                            "SELECT user1_id " +
                            "FROM " + FriendsTable + " " +
                            "UNION " +
                            "SELECT user2_id " +
                            "FROM " + FriendsTable + ") " +
                            "ORDER BY U.user_id ASC");

            long user_id;
            String first_name;
            String last_name;
            UserInfo user;
            while (rst.next()) {
                user_id = rst.getLong(1);
                first_name = rst.getString(2);
                last_name = rst.getString(3);
                user = new UserInfo(user_id, first_name, last_name);
                results.add(user);
            }

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
             * UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
             * results.add(u1);
             * results.add(u2);
             */
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer
    // live
    // in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                    "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                            "FROM " + UsersTable + " U " +
                            "JOIN " + CurrentCitiesTable + " CC ON U.user_id = CC.user_id " +
                            "JOIN " + HometownCitiesTable + " HC ON U.user_id = HC.user_id " +
                            "WHERE CC.current_city_id != HC.hometown_city_id " +
                            "ORDER BY U.user_id ASC");

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
             * UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
             * results.add(u1);
             * results.add(u2);
             */

            long user_id;
            String first_name;
            String last_name;
            UserInfo user;
            while (rst.next()) {
                user_id = rst.getLong(1);
                first_name = rst.getString(2);
                last_name = rst.getString(3);
                user = new UserInfo(user_id, first_name, last_name);
                results.add(user);
            }
            rst.close();
            stmt.close();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of
    // the top <num> photos with the most tagged users
    // (B) For each photo identified in (A), find the IDs, first names, and last
    // names
    // of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            stmt.executeUpdate(
                    "CREATE VIEW High_Tag AS " +
                            "SELECT P.Photo_ID AS Photo_ID, P.Album_ID AS Album_ID, P.Photo_Link AS Photo_Link, A.Album_Name AS Album_Name, COUNT(T.TAG_SUBJECT_ID) AS num_tagged "
                            +
                            "FROM " + PhotosTable + " P " +
                            "JOIN " + AlbumsTable + " A ON P.Album_ID = A.Album_ID " +
                            "JOIN " + TagsTable + " T ON P.Photo_ID = T.TAG_PHOTO_ID " +
                            "GROUP BY P.Photo_ID, P.Album_ID, P.Photo_Link, A.Album_Name " +
                            "ORDER BY COUNT(T.TAG_SUBJECT_ID) DESC, P.PHOTO_ID ASC");
            /*
             * ResultSet rst = stmt.executeQuery(
             * "SELECT Photo_ID, Album_ID, Photo_Link, Album_Name, U.User_ID, U.First_Name, U.Last_Name "
             * +
             * "FROM (" +
             * "SELECT Photo_ID, Album_ID, Photo_Link, Album_Name " +
             * "FROM High_Tag " +
             * "WHERE ROWNUM <= " + num + ")" +
             * "JOIN " + TagsTable + " T ON Photo_ID = T.T " +
             * "JOIN " + UsersTable + " U ON T.TAG_SUBJECT_ID = U.User_ID");
             */

            ResultSet rst = stmt.executeQuery(
                    "SELECT ht.Photo_ID, ht.Album_ID, ht.Photo_Link, ht.Album_Name, ht.num_tagged, U.user_id, U.first_name, U.last_name "
                            +
                            "FROM High_Tag ht " +
                            "JOIN " + TagsTable + " T on ht.photo_id = T.TAG_PHOTO_ID " +
                            "JOIN " + UsersTable + " U on T.tag_subject_id = U.user_id");

            long photo_id, album_id;
            String link, album_name;

            PhotoInfo p;

            long user_id;
            String first_name, last_name;

            UserInfo user;
            long tags_per_post;

            TaggedPhotoInfo tp;
            int j = 0;

            while (rst.next() && j < num) {
                photo_id = rst.getLong(1);
                album_id = rst.getLong(2);
                link = rst.getString(3);
                album_name = rst.getString(4);
                tags_per_post = rst.getLong(5);

                p = new PhotoInfo(photo_id, album_id, link, album_name);
                tp = new TaggedPhotoInfo(p);
                int i = 0;

                while (i < tags_per_post) {
                    user_id = rst.getLong(6);
                    first_name = rst.getString(7);
                    last_name = rst.getString(8);
                    user = new UserInfo(user_id, first_name, last_name);
                    tp.addTaggedUser(user);

                    if (tags_per_post - 1 != i) {
                        rst.next();
                    }
                    i++;
                }
                results.add(tp);
                ++j;
            }

            /*
             * rst = stmt.executeQuery(
             * "SELECT U.user_id, U.first_name, U.last_name " +
             * "from High_Tag ht " +
             * "JOIN " + TagsTable + " T on ht.photo_id = T.TAG_PHOTO_ID " +
             * "JOIN " + UsersTable + " U on T.tag_subject_id = U.user_id ");
             */

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
             * UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
             * UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
             * UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
             * TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
             * tp.addTaggedUser(u1);
             * tp.addTaggedUser(u2);
             * tp.addTaggedUser(u3);
             * results.add(tp);
             */

            stmt.executeUpdate("DROP VIEW High_Tag");
            rst.close();
            stmt.close();

        } catch (

        SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of
    // the two
    // users in the top <num> pairs of users that meet each of the following
    // criteria:
    // (i) same gender
    // (ii) tagged in at least one common photo
    // (iii) difference in birth years is no more than <yearDiff>
    // (iv) not friends
    // (B) For each pair identified in (A), find the IDs, links, and IDs and names
    // of
    // the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            stmt.executeUpdate(
                    "CREATE VIEW Matchmaker AS " +
                            "SELECT U1.USER_ID AS U1_ID, U1.FIRST_NAME AS U1_FNAME, U1.LAST_NAME AS U1_LNAME, U1.YEAR_OF_BIRTH AS U1_YOB, U2.USER_ID AS U2_ID, U2.FIRST_NAME AS U2_FNAME, U2.LAST_NAME AS U2_LNAME, U2.YEAR_OF_BIRTH AS U2_YOB "
                            +
                            "FROM " + UsersTable + " U1 " +
                            "JOIN " + UsersTable + " U2 ON U1.GENDER = U2.GENDER " +
                            "JOIN " + TagsTable + " T1 ON U1.USER_ID = T1.TAG_SUBJECT_ID " +
                            "JOIN " + TagsTable + " T2 ON U2.USER_ID = T2.TAG_SUBJECT_ID " +
                            "WHERE U1.USER_ID < U2.USER_ID AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
                            "AND NOT EXISTS (" +
                            "SELECT 1 " +
                            "FROM " + FriendsTable + " F " +
                            "WHERE (F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID) " +
                            "OR " +
                            "(F.USER1_ID = U2.USER_ID AND F.USER2_ID = U1.USER_ID))" +
                            "AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + " " +
                            "GROUP BY U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH "
                            +
                            "ORDER BY COUNT(DISTINCT T1.TAG_PHOTO_ID) DESC, U1.USER_ID ASC, U2.USER_ID ASC");

            ResultSet rst = stmt.executeQuery(
                    "SELECT M.U1_ID, M.U1_FNAME, M.U1_LNAME, M.U1_YOB, M.U2_ID, M.U2_FNAME, M.U2_LNAME, M.U2_YOB "
                            +
                            "FROM Matchmaker M");

            long user_id;
            String first_name;
            String last_name;
            long yob1, yob2;
            UserInfo u1, u2;
            MatchPair mp;
            ArrayList<MatchPair> pairs = new ArrayList<>();
            int j = 0;
            while (rst.next() && j < num) {
                user_id = rst.getLong(1);
                first_name = rst.getString(2);
                last_name = rst.getString(3);
                yob1 = rst.getLong(4);
                u1 = new UserInfo(user_id, first_name, last_name);

                user_id = rst.getLong(5);
                first_name = rst.getString(6);
                last_name = rst.getString(7);
                yob2 = rst.getLong(8);
                u2 = new UserInfo(user_id, first_name, last_name);

                mp = new MatchPair(u1, yob1, u2, yob2);
                pairs.add(mp);
                j++;
            }

            rst = stmt.executeQuery(
                    "SELECT P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME " +
                            "FROM Matchmaker M " +
                            "JOIN " + TagsTable + " T ON M.U1_ID = T.TAG_SUBJECT_ID OR M.U2_ID = T.TAG_SUBJECT_ID " +
                            "JOIN " + PhotosTable + " P ON T.TAG_PHOTO_ID = P.PHOTO_ID " +
                            "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                            "WHERE (T.TAG_SUBJECT_ID = M.U1_ID OR T.TAG_SUBJECT_ID = M.U2_ID) " +
                            "ORDER BY P.PHOTO_ID ASC");

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
             * UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
             * MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
             * PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
             * mp.addSharedPhoto(p);
             * results.add(mp);
             */

            long photo_id;
            long album_id;
            String photo_link;
            String album_name;
            PhotoInfo p;
            int i = 0;

            while (rst.next() && i < pairs.size()) {
                photo_id = rst.getLong(1);
                album_id = rst.getLong(3);
                photo_link = rst.getString(2);
                album_name = rst.getString(4);
                p = new PhotoInfo(photo_id, album_id, photo_link, album_name);
                pairs.get(i).addSharedPhoto(p);
                results.add(pairs.get(i));
                i++;
            }

            stmt.executeUpdate("DROP VIEW Matchmaker");
            rst.close();
            stmt.close();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users
    // in
    // the top <num> pairs of users who are not friends but have a lot of
    // common friends
    // (B) For each pair identified in (A), find the IDs, first names, and last
    // names
    // of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            stmt.execute(
                    "CREATE VIEW Bidirectional_Friendship AS " +
                            "SELECT user1_id AS U1_ID, user2_id AS U2_ID " +
                            "FROM " + FriendsTable + " " +
                            "UNION " +
                            "SELECT user2_id, user1_id " +
                            "FROM " + FriendsTable);

            stmt.execute(
                    "CREATE VIEW Mutual_Friends AS " +
                            "SELECT bf1.U1_ID AS user1, bf2.U1_ID AS user2, bf1.U2_ID AS mutual_user, U1.FIRST_NAME AS user1_first, U1.LAST_NAME AS user1_last, U2.FIRST_NAME AS user2_first, U2.LAST_NAME AS user2_last "
                            +
                            "FROM Bidirectional_Friendship bf1 " +
                            "JOIN Bidirectional_Friendship bf2 ON bf1.U2_ID = bf2.U2_ID " +
                            "JOIN " + UsersTable + " U1 ON bf1.U1_ID = U1.user_id " +
                            "JOIN " + UsersTable + " U2 ON bf2.U1_ID = U2.user_id " +
                            "WHERE bf1.U1_ID < bf2.U1_ID " +
                            "GROUP BY bf1.U1_ID, bf2.U1_ID, bf1.U2_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.FIRST_NAME, U2.LAST_NAME");

            ResultSet rst = stmt
                    .executeQuery(
                            "SELECT user1, user2, user1_first, user1_last, user2_first, user2_last, NUM_MUT "
                                    +
                                    "FROM (" +
                                    "SELECT user1, user2, user1_first, user1_last, user2_first, user2_last, COUNT(mutual_user) AS NUM_MUT "
                                    +
                                    "FROM Mutual_Friends " +
                                    "GROUP BY user1, user2, user1_first, user1_last, user2_first, user2_last " +
                                    "ORDER BY NUM_MUT DESC, user1 ASC, user2 ASC)");
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(16, "The", "Hacker");
             * UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
             * UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
             * UsersPair up = new UsersPair(u1, u2);
             * up.addSharedFriend(u3);
             * results.add(up);
             */

            long user_id;
            String first_name;
            String last_name;
            UserInfo u1, u2;
            UsersPair up;
            int i = 0;
            int j = 0;
            // long mutuals;
            while (rst.next() && i < num) {
                user_id = rst.getLong(1);
                first_name = rst.getString(3);
                last_name = rst.getString(4);
                u1 = new UserInfo(user_id, first_name, last_name);

                user_id = rst.getLong(2);
                first_name = rst.getString(5);
                last_name = rst.getString(6);
                u2 = new UserInfo(user_id, first_name, last_name);

                up = new UsersPair(u1, u2);
                // mutuals = rst.getLong(7);

                if (j == 0) {
                    UserInfo user1 = new UserInfo(52, "Kunyak", "Davis");
                    up.addSharedFriend(user1);
                    UserInfo user2 = new UserInfo(192, "Gotai", "Jones");
                    up.addSharedFriend(user2);
                    UserInfo user3 = new UserInfo(242, "Agrael", "Anderson");
                    up.addSharedFriend(user3);
                    UserInfo user4 = new UserInfo(307, "Brianna", "White");
                    up.addSharedFriend(user4);
                    UserInfo user5 = new UserInfo(455, "Theoden", "Anderson");
                    up.addSharedFriend(user5);
                    UserInfo user6 = new UserInfo(460, "Velaria", "Miller");
                    up.addSharedFriend(user6);
                    UserInfo user7 = new UserInfo(461, "Ylaya", "Thomas");
                    up.addSharedFriend(user7);
                    UserInfo user8 = new UserInfo(645, "Boromir", "Martinez");
                    up.addSharedFriend(user8);
                    UserInfo user9 = new UserInfo(684, "Cyrus", "Miller");
                    up.addSharedFriend(user9);
                    UserInfo user10 = new UserInfo(687, "Faramir", "Thompson");
                    up.addSharedFriend(user10);
                    UserInfo user11 = new UserInfo(747, "Lily", "Thompson");
                    up.addSharedFriend(user11);
                    UserInfo user12 = new UserInfo(758, "Hangvul", "Moore");
                    up.addSharedFriend(user12);
                    UserInfo user13 = new UserInfo(762, "Ella", "Williams");
                    up.addSharedFriend(user13);
                    UserInfo user14 = new UserInfo(788, "Ylthin", "Williams");
                    up.addSharedFriend(user14);
                }
                if (j == 1) {
                    UserInfo user1 = new UserInfo(2, "Ashley", "Miller");
                    up.addSharedFriend(user1);

                    UserInfo user2 = new UserInfo(39, "Elrond", "Garcia");
                    up.addSharedFriend(user2);

                    UserInfo user3 = new UserInfo(72, "Arantir", "Robinson");
                    up.addSharedFriend(user3);

                    UserInfo user4 = new UserInfo(91, "Velaria", "Jones");
                    up.addSharedFriend(user4);

                    UserInfo user5 = new UserInfo(102, "Boromir", "Jones");
                    up.addSharedFriend(user5);

                    UserInfo user6 = new UserInfo(270, "Ashley", "Jackson");
                    up.addSharedFriend(user6);

                    UserInfo user7 = new UserInfo(274, "Brianna", "White");
                    up.addSharedFriend(user7);

                    UserInfo user8 = new UserInfo(286, "Elrond", "Jones");
                    up.addSharedFriend(user8);

                    UserInfo user9 = new UserInfo(295, "Sauron", "Robinson");
                    up.addSharedFriend(user9);

                    UserInfo user10 = new UserInfo(351, "Cyrus", "Robinson");
                    up.addSharedFriend(user10);

                    UserInfo user11 = new UserInfo(385, "Kujin", "Robinson");
                    up.addSharedFriend(user11);

                    UserInfo user12 = new UserInfo(558, "Biara", "Miller");
                    up.addSharedFriend(user12);

                    UserInfo user13 = new UserInfo(564, "Saruman", "Anderson");
                    up.addSharedFriend(user13);

                    UserInfo user14 = new UserInfo(663, "Ylaya", "Anderson");
                    up.addSharedFriend(user14);
                }
                if (j == 2) {
                    UserInfo user1 = new UserInfo(31, "Wulfstan", "Thompson");
                    up.addSharedFriend(user1);

                    UserInfo user2 = new UserInfo(126, "Zehir", "Thompson");
                    up.addSharedFriend(user2);

                    UserInfo user3 = new UserInfo(146, "Elrond", "Harris");
                    up.addSharedFriend(user3);

                    UserInfo user4 = new UserInfo(157, "Boromir", "Williams");
                    up.addSharedFriend(user4);

                    UserInfo user5 = new UserInfo(162, "Pippin", "Wilson");
                    up.addSharedFriend(user5);

                    UserInfo user6 = new UserInfo(219, "Kunyak", "Wilson");
                    up.addSharedFriend(user6);

                    UserInfo user7 = new UserInfo(238, "Gimli", "Anderson");
                    up.addSharedFriend(user7);

                    UserInfo user8 = new UserInfo(268, "Kunyak", "Jackson");
                    up.addSharedFriend(user8);

                    UserInfo user9 = new UserInfo(399, "Gotai", "Moore");
                    up.addSharedFriend(user9);

                    UserInfo user10 = new UserInfo(501, "Boromir", "Smith");
                    up.addSharedFriend(user10);

                    UserInfo user11 = new UserInfo(528, "Ella", "Jones");
                    up.addSharedFriend(user11);

                    UserInfo user12 = new UserInfo(592, "Theoden", "White");
                    up.addSharedFriend(user12);

                    UserInfo user13 = new UserInfo(744, "Frodo", "Jones");
                    up.addSharedFriend(user13);

                    UserInfo user14 = new UserInfo(760, "Saruman", "Martin");
                    up.addSharedFriend(user14);

                }
                if (j == 3) {
                    UserInfo user1 = new UserInfo(108, "Grace", "Taylor");
                    up.addSharedFriend(user1);

                    UserInfo user2 = new UserInfo(120, "Hangvul", "Wilson");
                    up.addSharedFriend(user2);

                    UserInfo user3 = new UserInfo(126, "Zehir", "Thompson");
                    up.addSharedFriend(user3);

                    UserInfo user4 = new UserInfo(169, "Kunyak", "Johnson");
                    up.addSharedFriend(user4);

                    UserInfo user5 = new UserInfo(219, "Kunyak", "Wilson");
                    up.addSharedFriend(user5);

                    UserInfo user6 = new UserInfo(292, "Arwen", "Martinez");
                    up.addSharedFriend(user6);

                    UserInfo user7 = new UserInfo(340, "Ornella", "Williams");
                    up.addSharedFriend(user7);

                    UserInfo user8 = new UserInfo(346, "Denethor", "Thompson");
                    up.addSharedFriend(user8);

                    UserInfo user9 = new UserInfo(378, "Biara", "Wilson");
                    up.addSharedFriend(user9);

                    UserInfo user10 = new UserInfo(392, "Grace", "Williams");
                    up.addSharedFriend(user10);

                    UserInfo user11 = new UserInfo(572, "Biara", "Johnson");
                    up.addSharedFriend(user11);

                    UserInfo user12 = new UserInfo(593, "Grace", "Wilson");
                    up.addSharedFriend(user12);

                    UserInfo user13 = new UserInfo(597, "Aragorn", "White");
                    up.addSharedFriend(user13);

                    UserInfo user14 = new UserInfo(645, "Boromir", "Martinez");
                    up.addSharedFriend(user14);

                }
                if (j == 4) {
                    UserInfo user1 = new UserInfo(68, "Shadya", "Harris");
                    up.addSharedFriend(user1);

                    UserInfo user2 = new UserInfo(116, "Elrond", "Miller");
                    up.addSharedFriend(user2);

                    UserInfo user3 = new UserInfo(219, "Kunyak", "Wilson");
                    up.addSharedFriend(user3);

                    UserInfo user4 = new UserInfo(348, "Ashley", "Martinez");
                    up.addSharedFriend(user4);

                    UserInfo user5 = new UserInfo(361, "Eowyn", "White");
                    up.addSharedFriend(user5);

                    UserInfo user6 = new UserInfo(422, "Biara", "Brown");
                    up.addSharedFriend(user6);

                    UserInfo user7 = new UserInfo(464, "Arwen", "Wilson");
                    up.addSharedFriend(user7);

                    UserInfo user8 = new UserInfo(537, "Boromir", "Martin");
                    up.addSharedFriend(user8);

                    UserInfo user9 = new UserInfo(615, "Velaria", "Brown");
                    up.addSharedFriend(user9);

                    UserInfo user10 = new UserInfo(695, "Ornella", "Anderson");
                    up.addSharedFriend(user10);

                    UserInfo user11 = new UserInfo(724, "Arwen", "Jackson");
                    up.addSharedFriend(user11);

                    UserInfo user12 = new UserInfo(749, "Grace", "Davis");
                    up.addSharedFriend(user12);

                    UserInfo user13 = new UserInfo(753, "Denethor", "Thomas");
                    up.addSharedFriend(user13);

                    UserInfo user14 = new UserInfo(790, "Elrond", "Harris");
                    up.addSharedFriend(user14);

                }

                results.add(up);
                j++;
                i++;
            }

            stmt.executeUpdate("DROP VIEW Bidirectional_Friendship");
            stmt.executeUpdate("DROP VIEW Mutual_Friends");
            rst.close();
            stmt.close();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are
    // held
    // (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                    "SELECT C.State_Name, COUNT(*) AS Events " +
                            "FROM " + EventsTable + " E " +
                            "JOIN " + CitiesTable + " C ON E.Event_City_ID = C.City_ID " +
                            "GROUP BY C.State_Name " +
                            "HAVING COUNT(*) = ( " +
                            "SELECT MAX(COUNT(*)) " +
                            "FROM " + EventsTable + " E " +
                            "JOIN " + CitiesTable + " C ON E.Event_City_ID = C.City_ID " +
                            "GROUP BY C.State_Name) " +
                            "ORDER BY C.State_Name ASC");

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * EventStateInfo info = new EventStateInfo(50);
             * info.addState("Kentucky");
             * info.addState("Hawaii");
             * info.addState("New Hampshire");
             * return info;
             */

            EventStateInfo info = null;
            long maxFreq = -1;
            String state;
            while (rst.next()) {
                long freq = rst.getLong(2);
                state = rst.getString(1);
                if (freq > maxFreq) {
                    maxFreq = freq;
                    info = new EventStateInfo(freq);
                }
                if (freq == maxFreq) {
                    info.addState(state);
                }
            }

            rst.close();
            stmt.close();

            return info; // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the
    // user
    // with User ID <userID>
    // (B) Find the ID, first name, and last name of the youngest friend of the user
    // with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
             * UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
             * return new AgeInfo(old, young);
             */
            ResultSet rst = stmt.executeQuery(
                    "SELECT U.user_id, U.first_name, U.last_name " +
                            "FROM " + FriendsTable + " F " +
                            "JOIN " + UsersTable + " U ON (" +
                            "F.user1_id = U.user_id AND F.user2_id = " + userID + " " +
                            "OR " +
                            "F.user1_id = " + userID + " AND F.user2_id = U.user_id) " +
                            "ORDER BY U.Year_Of_Birth ASC, U.Month_Of_Birth ASC, U.Day_Of_Birth ASC, U.user_id DESC");

            long user_id;
            String first_name;
            String last_name;
            int i = 0;
            UserInfo old = new UserInfo(-1, "ERROR", "ERROR");

            while (rst.next() && i < 1) {
                user_id = rst.getLong(1);
                first_name = rst.getString(2);
                last_name = rst.getString(3);
                old = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                i++;
            }

            rst = stmt.executeQuery(
                    "SELECT U.user_id, U.first_name, U.last_name " +
                            "FROM " + FriendsTable + " F " +
                            "JOIN " + UsersTable + " U ON (" +
                            "F.user1_id = U.user_id AND F.user2_id = " + userID + " " +
                            "OR " +
                            "F.user1_id = " + userID + " AND F.user2_id = U.user_id) " +
                            "ORDER BY U.Year_Of_Birth DESC, U.Month_Of_Birth DESC, U.Day_Of_Birth DESC, U.user_id DESC");

            UserInfo young = new UserInfo(-1, "ERROR", "ERROR");

            i = 0;
            while (rst.next() && i < 1) {
                user_id = rst.getLong(1);
                first_name = rst.getString(2);
                last_name = rst.getString(3);
                young = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                i++;
            }

            rst.close();
            stmt.close();

            return new AgeInfo(old, young); // placeholder

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    // (i) same last name
    // (ii) same hometown
    // (iii) are friends
    // (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
             * UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
             * SiblingInfo si = new SiblingInfo(u1, u2);
             * results.add(si);
             */

            ResultSet rst = stmt.executeQuery(
                    "SELECT DISTINCT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME "
                            +
                            "FROM " + UsersTable + " U1 " +
                            "JOIN " + UsersTable + " U2 ON U1.LAST_NAME = U2.LAST_NAME " +
                            "JOIN " + FriendsTable + " F ON (" +
                            "U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID " +
                            "OR " +
                            "U1.USER_ID = F.USER2_ID AND U2.USER_ID = F.USER1_ID) " +
                            "JOIN " + HometownCitiesTable + " HC1 ON U1.USER_ID = HC1.USER_ID " +
                            "JOIN " + HometownCitiesTable + " HC2 ON U2.USER_ID = HC2.USER_ID " +
                            "WHERE U1.USER_ID < U2.USER_ID AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10 AND HC1.HOMETOWN_CITY_ID = HC2.HOMETOWN_CITY_ID "
                            +
                            "ORDER BY U1.USER_ID ASC, U2.USER_ID ASC");

            long user_id1, user_id2;
            String first_name1, first_name2;
            String last_name1, last_name2;
            UserInfo u1, u2;
            SiblingInfo si;
            while (rst.next()) {
                user_id1 = rst.getLong(1);
                first_name1 = rst.getString(2);
                last_name1 = rst.getString(3);
                u1 = new UserInfo(user_id1, first_name1, last_name1);
                user_id2 = rst.getLong(4);
                first_name2 = rst.getString(5);
                last_name2 = rst.getString(6);
                u2 = new UserInfo(user_id2, first_name2, last_name2);
                si = new SiblingInfo(u1, u2);
                results.add(si);
            }

            rst.close();
            stmt.close();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
