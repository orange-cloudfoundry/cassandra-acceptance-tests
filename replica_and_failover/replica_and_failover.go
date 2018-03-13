package replica_and_failover_tests

import (
	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	//"github.com/satori/go.uuid"
	//"time"
	//testing
	"bytes"
	"io"
	"os"
	"os/exec"
)

var _ = Describe("Cassandra replica_and_failover_tests", func() {
	var err error
	var session, session1, session2, session3 *gocql.Session
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
	var tableName = "tableName1" // + differentiator
	var col1 = "col1"            //+ differentiator
	var col2 = "col2"            //+ differentiator
	var data1 = "data1"          //+ differentiator
	var data2 = "data2"          //+ differentiator
	var data3 = "data3"          //+ differentiator
	var tableCreation = fmt.Sprintf("CREATE TABLE  %s (%s text PRIMARY KEY , %s text)", tableName, col1, col2)
	var dataInsertion = fmt.Sprintf("INSERT INTO %s ( %s  , %s ) VALUES ('%s' , '%s' )", tableName, col1, col2, data1, data2)
	var tableDrop = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	var updateData = fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE %s = '%s'", tableName, col2, data3, col1, data1)
	var deleteData = fmt.Sprintf("DELETE FROM %s WHERE %s = '%s'", tableName, col1, data1)
	var showData = fmt.Sprintf("SELECT * FROM %s", tableName)

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
		cluster.Consistency = ALL
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

		By("reconnecting to the session and the keyspace as the new user")
		cluster.Authenticator = authNewUser
		session, err = cluster.CreateSession()
		Expect(err).NotTo(HaveOccurred())

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

		By("disconnecting from the session as the new User")
		session.Close()

		By("reconnecting to the session as superuser")
		cluster.Authenticator = authSuperUser
		session, err = cluster.CreateSession()
		Expect(err).NotTo(HaveOccurred())

		By("dropping the new user")
		err := session.Query(dropUser).Exec()
		Expect(err).NotTo(HaveOccurred())

		By("dropping the new keyspace")
		err = session.Query(dropKeySpace).Exec()
		Expect(err).NotTo(HaveOccurred())

		By("disconnecting from the session as superuser")
		session.Close()
	})

	Context("testsssssss", func() {
		It("should test alright", func() {
		cmd1:
			exec.Command("$CASSANDRA_BIN/cqlsh", "--cqlshrc", "-u", "cassandra", "-p", config.CassPwd)
		cm2:
			exec.Command

			Expect(1).To(Equal(1))

		})

	})
	/*
		Context("When the replication factor equals 3", func() {

			It("should verify that the data is present on each node", func() {
				if len(config.Seeds) != 2 || len(config.Servers) != 2 || config.RfFactor != "3" != 2 { //add ressurector off condition
					Skip("incorrect cluster config")
				}

				It("should find an existing document", func() {
					err = session.Query(findData1)
					Expect(err).NotTo(HaveOccurred())
					if err == nil {
						count = session.Query(findData1).Iter().NumRows()
						Expect(count).To(Equal(1))
					}
				})
			})
		}) */
	/*
		Context("When provoking a soft failover with 1 seed and 2 servers", func() {
			BeforeEach(func() {
				if len(config.Seeds) != 1 || len(config.Servers) != 2 {
					return
				}
				By("killing one seed node")
				cmdName := "/var/vcap/packages/cassandra/bin/nodetool"
				cmdArgs := []string{"-h", config.seeds[1], "-p", CassAdminPwd, "-u", "cassandra", "stopdaemon", config.seeds[0]}
				cmd := exec.Command(cmdName, cmdArgs...)
				query, err := cmd.CombinedOutput()
				Expect(err).NotTo(HaveOccured())
			})

			Context("When the resurrector is turned-off", func() {

				It("should verify a CQL request gives the same result before and after the node killing", func() {
					if len(config.Seeds) != 1 || len(config.Servers) != 2 { //add ressurector off condition
						Skip("incorrect cluster config")
					}
					err = session.Query(findData1)
					Expect(err).NotTo(HaveOccurred())
					if err == nil {
						count = session.Query(findData1).Iter().NumRows()
						Expect(count).To(Equal(1))
					}
				})

			})
			Context("When the resurrector is turned-on", func() {

				It("should verify the resuscitated seed node re-joined the cluster", func() {
					if len(config.Seeds) != 1 || len(config.Servers) != 2 { //add ressurector on condition
						Skip("incorrect cluster config")
					}
					session.Close()
					Cluster.Keyspace = "system"
					session, err = cluster.CreateSession()
					ect(err).NotTo(HaveOccurred())
					err = session.Query(findHintDropped)
					Expect(err).NotTo(HaveOccurred())
					count = session.Query(findHintDropped).Iter().NumRows()
					Expect(count).To(Equal(1))
				})

				It("should verify the data is present on the resuscitated seed node", func() {
					if len(config.Seeds) != 1 || len(config.Servers) != 2 { //add ressurector on condition
						Skip("incorrect cluster config")
					}
					err = session.Query(findData1)
					Expect(err).NotTo(HaveOccurred())
					if err == nil {
						count = session.Query(findData1).Iter().NumRows()
						Expect(count).To(Equal(1))
					}
				})
			})

		})
		Context("When provoking a dirty failover of a seed with 2 seeds/2servers and the ressurector is on", func() {

			BeforeEach(func() {
				if len(config.Seeds) != 2 || len(config.Servers) != 2 { //add ressurector on condition
					return
				}
				By("killing one seed node")
				cmdName := "/var/vcap/packages/cassandra/bin/nodetool"
				cmdArgs := []string{"-h", config.seeds[1], "-p", CassAdminPwd, "-u", "cassandra", "assassinate", config.seeds[0]}
				cmd := exec.Command(cmdName, cmdArgs...)
				query, err := cmd.CombinedOutput()
				Expect(err).NotTo(HaveOccured())
			})

			Context("When the ressurector is off")

			It("should verify the last seed node takes", func() {
				if len(config.Seeds) != 2 || len(config.Servers) != 2 { //add ressurector on condition
					Skip("incorrect cluster config or ressurectoris off")
				}
			})

			It("should verify the rest of the cluster is available", func() {
				if len(config.Seeds) != 2 || len(config.Servers) != 2 { //add ressurector on condition
					Skip("incorrect cluster config or ressurectoris off")
				}
			})

		}) */
})
