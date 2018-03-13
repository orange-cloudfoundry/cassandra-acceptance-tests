package crud_tests

import (
	"fmt"
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	//"github.com/satori/go.uuid"
	"time"
)

var _ = Describe("MongoDB CRUD tests", func() {

	var err error
	var session *gocql.Session
	var cluster *gocql.ClusterConfig
	var clusterAdress = config.Seeds[0]
	//var differentiator, err := uuid.NewV4().String()
	var nameNewUser = "newUser" //+ differentiator
	var pwdNewUser = "pwd"      //+ differentiator
	var keyspaceName = "toto"   //+ differentiator
	var authSuperUser = gocql.PasswordAuthenticator{"cassandra", config.CassPwd}
	var authNewUser = gocql.PasswordAuthenticator{nameNewUser, pwdNewUser}
	var strat = fmt.Sprintf("{ 'class' : '%s', 'replication_factor' : %d }", config.ReplStrat, config.RfFactor)
	var createKeySpace = fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s AND DURABLE_WRITES = %t", keyspaceName, strat, config.DurableW)
	var createUser = fmt.Sprintf("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s' SUPERUSER", nameNewUser, pwdNewUser)
	var dropUser = "DROP USER IF EXISTS nameNewUser"
	var dropKeySpace = "DROP KEYSPACE IF EXISTS " + keyspaceName

	BeforeEach(func() {

		By("finding the cluster")
		cluster = gocql.NewCluster(clusterAdress)

		By("setting Authenticator as superuser")
		cluster.Authenticator = authSuperUser

		By("setting a timeout")
		cluster.Timeout = 60 * time.Second

		By("creating an initial keyspace")
		cluster.Keyspace = "system"
		cluster.ProtoVersion = 4
		cluster.ConnectTimeout = 10 * time.Second
		session, err = cluster.CreateSession()
		Expect(err).NotTo(HaveOccurred())
		err = session.Query(createKeySpace).Exec()
		if err != nil {
			fmt.Println("failed connexion to keyspace system", err)
		} else {
			fmt.Println("successful connexion to system")
		}
		Expect(err).NotTo(HaveOccurred())
		session.Close()

		By("using the created keyspace as a superuser")
		cluster.Keyspace = keyspaceName
		session, err = cluster.CreateSession()
		if err != nil {
			fmt.Println("failed creation keyspace")
		}
		Expect(err).NotTo(HaveOccurred())

		By("creating a new non-superuser, non-admin user") //for the time being it s a superuser create a insert delete select role later
		err = session.Query(createUser).Exec()
		Expect(err).NotTo(HaveOccurred())

		By("closing the session")
		session.Close()
	})

	AfterEach(func() {
		By("connecting to the session as a superuser")
		cluster.Authenticator = authSuperUser
		session, err = cluster.CreateSession()
		Expect(err).NotTo(HaveOccurred())

		By("dropping the user")
		err := session.Query(dropUser).Exec()
		Expect(err).NotTo(HaveOccurred())

		By("dropping the keyspace")
		err = session.Query(dropKeySpace).Exec()
		Expect(err).NotTo(HaveOccurred())

		By("disconnecting from the session")
		session.Close()
	})
	Context("When a user is created", func() {
		var tableName = "tableName1" // + differentiator
		var col1 = "col1"            //+ differentiator
		var col2 = "col2"            //+ differentiator
		var data1 = "data1"          //+ differentiator
		var data2 = "data2"          //+ differentiator
		var data3 = "data3"          //+ differentiator

		BeforeEach(func() {

			By("reconnecting to the session as the new user")
			cluster.Authenticator = authNewUser
			session, err = cluster.CreateSession()
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			By("disconnecting from the session as the new User")
			session.Close()
		})

		Context("When connected to a database as user", func() {
			var tableCreation = fmt.Sprintf("CREATE TABLE  %s (%s text PRIMARY KEY , %s text)", tableName, col1, col2)
			var dataInsertion = fmt.Sprintf("INSERT INTO %s ( %s  , %s ) VALUES ('%s' , '%s' )", tableName, col1, col2, data1, data2)
			var tableDrop = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
			var updateData = fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE %s = '%s'", tableName, col2, data3, col1, data1)
			var deleteData = fmt.Sprintf("DELETE FROM %s WHERE %s = '%s'", tableName, col1, data1)
			var showData = fmt.Sprintf("SELECT * FROM %s", tableName)

			BeforeEach(func() {
				By("creating a table")
				err = session.Query(tableCreation).Exec()
				Expect(err).NotTo(HaveOccurred())

				By("inserting data in this table")
				err = session.Query(dataInsertion).Exec()
				Expect(err).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				By("dropping the table")
				err = session.Query(tableDrop).Exec()
				Expect(err).NotTo(HaveOccurred())
			})

			It("should find an existing document", func() {
				err = session.Query(showData).Exec()
				Expect(err).NotTo(HaveOccurred())
				iter := session.Query(showData).Iter()
				s, err := iter.SliceMap()
				Expect(err).NotTo(HaveOccurred())
				m1 := s[0][col1]
				m2 := s[0][col2]
				Expect(m1).To(Equal(data1))
				Expect(m2).To(Equal(data2))
			})

			It("should update an existing document", func() {
				err = session.Query(updateData).Exec()
				Expect(err).NotTo(HaveOccurred())
				err = session.Query(showData).Exec()
				Expect(err).NotTo(HaveOccurred())
				iter := session.Query(showData).Iter()
				s, err := iter.SliceMap()
				Expect(err).NotTo(HaveOccurred())
				m1 := s[0][col1]
				m2 := s[0][col2]
				Expect(m1).To(Equal(data1))
				Expect(m2).To(Equal(data3))
			})

			It("should delete an existing document", func() {
				fmt.Println(deleteData)
				err = session.Query(deleteData).Exec()
				Expect(err).NotTo(HaveOccurred())
				err = session.Query(showData).Exec()
				Expect(err).NotTo(HaveOccurred())
				iter := session.Query(showData).Iter()
				s, err := iter.SliceMap()
				Expect(err).NotTo(HaveOccurred())
				v := []map[string]interface{}{}
				Expect(s).To(Equal(v))
			})
		})
	})
})
