// Mongo's first start: a database and a user for each deployment (allowed only its own
// database), its greeting in it.
const fs = require("fs");
for (const env of ["dev", "prod"]) {
  const database = db.getSiblingDB(env);
  // In `admin`, where a client authenticates when its URI names no `authSource`.
  db.getSiblingDB("admin").createUser({
    user: `${env}_app`,
    pwd: fs.readFileSync(`/demo/servers/${env}/password`, "utf8").trim(),
    roles: [{ role: "readWrite", db: env }],
  });
  database.greetings.insertOne({ greeting: `hello from ${env}'s Mongo` });
}
