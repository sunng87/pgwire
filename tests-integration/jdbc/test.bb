(require '[babashka.pods :as pods])
(pods/load-pod 'org.babashka/postgresql "0.1.1")

(require '[pod.babashka.postgresql :as pg])

(def db {:dbtype   "postgresql"
         :host     "127.0.0.1"
         :dbname   "localdb"
         :user     "postgres"
         :password "pencil"
         :port     5432})

(println (pg/execute! db ["INSERT INTO testtable VALUES (1)"]))

(println (pg/execute! db ["SELECT * FROM testable"]))

(println (pg/execute! db ["SELECT * FROM testable WHERE id = ?" 1]))
