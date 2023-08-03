const { Kafka } = require("kafkajs");

const msg = process.argv[2]; // first argument as message

const MyPartitioner = () => {
	return ({ message }) => {
		return message.value[0].toLowerCase() < "n" ? 0 : 1;
	};
};

async function run() {
	try {
		// Create a kafka object
		const kafka = new Kafka({
			clientId: "myapp",
			brokers: ["localhost:29092"],
		});

		// get the producer object
		const producer = kafka.producer({
			createPartitioner: MyPartitioner, // custom partitioner (https://kafka.js.org/docs/producing#custom-partitioner)
		});

		console.log("Connecting...");
		await producer.connect();
		console.log("Connected!");

		// publish to topic
		const result = await producer.send({
			topic: "Users",
			messages: [
				{
					value: msg,
				},
			],
		});
		console.log("Sent successfully! ", result);

		await producer.disconnect();
	} catch (e) {
		console.error("ERROR: ", e);
	} finally {
		process.exit();
	}
}

run();
