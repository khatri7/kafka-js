const { Kafka } = require("kafkajs");

const msg = process.argv[2]; // first argument as message

async function run() {
	try {
		// Create a kafka object
		const kafka = new Kafka({
			clientId: "myapp",
			brokers: ["localhost:29092"],
		});

		// get the consumer object
		const consumer = kafka.consumer({
			groupId: "test",
		});

		console.log("Connecting...");
		await consumer.connect();
		console.log("Connected!");

		// subscribe to a topic
		await consumer.subscribe({
			topic: "Users",
			fromBeginning: true,
		});

		await consumer.run({
			eachMessage: async (result) => {
				console.log(
					`Partition ${result.partition}: RECEIVED:: ${result.message.value}`
				);
			},
		});
	} catch (e) {
		console.error("ERROR: ", e);
	}
}

run();
