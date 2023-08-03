const { Kafka } = require("kafkajs");

async function run() {
	try {
        // Create a kafka object
		const kafka = new Kafka({
			clientId: "myapp",
			brokers: ["localhost:29092"],
		});

        // get the admin object
		const admin = kafka.admin();

		console.log("Connecting...");
		await admin.connect();
		console.log("Connected!");

		// create topic with 2 partitions -> A-M and N-Z
		await admin.createTopics({
			topics: [
				{
					topic: "Users",
					numPartitions: 2,
				},
			],
		});
		console.log("Topic created successfully!");

		await admin.disconnect();
	} catch (e) {
		console.error("ERROR: ", e);
	} finally {
		process.exit();
	}
}

run();
