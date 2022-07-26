require("dotenv").config();

let nforce = require("nforce");
let faye = require("faye");
let express = require("express");
let cors = require("cors");
const e = require("express");
let app = express();
let server = require("http").Server(app);
let io = require("socket.io")(server);

let muxJobs = [];
function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

let getMuxJobs = (req, res) => {
  // q is some SOQL query
  /*let q ="";
    org.query({ query: q }, (err, resp) => {
    if (err) {
      console.log(err);
      res.sendStatus(500);
    } else {
      let soqlJobs = resp.records;
      let returnJobs = [];
      soqlJobs.forEach((soqlJob) => {
        returnJobs.push({
          jobId: soqlJob.get("Id"),
          amount: soqlJob.get("Amount"),
        });
      });
      res.json(returnJobs);
    }
  });*/
  res.json(muxJobs);
};

/*let getJobDetails = (req, res) => {
  let jobId = req.params.jobId;
  // q is some SOQL query across multiple objects maybe
  let q ="";
  org.query({ query: q }, (err, resp) => {
    if (err) {
      console.log(err);
      res.sendStatus(500);
    } else {
      let soqlItems = resp.records;
      let returnItems = [];
      soqlItems.forEach((soqlItem) => {
        returnItems.push({
          jobId: soqlItem.get("Id"),
        });
      });
      res.json(returnItems);
    }
  });
};*/

let sendMuxInboundException = (req, res) => {
  let jobId = req.params.jobId;
  let event = nforce.createSObject("MUX_Inbound__e");
  event.set("MUX_Job_ID__c", jobId);
  event.set("Error_Message__c", "This job is exceptional");
  org.insert({ sobject: event }, (err) => {
    if (err) {
      console.error(err);
      res.sendStatus(500);
    } else {
      res.sendStatus(200);
    }
  });
};

// Can create MUX_Inbound_Job_Status__e or MUX_Inbound__e types
const createPlatformEvent = async (jobId, event_type, msgTxt) => {
  let event = nforce.createSObject(event_type);
  event.set("MUX_Job_ID__c", jobId);
  if (event_type === "MUX_Inbound_Job_Status__e") {
    event.set("Status__c", msgTxt);
  } else if (event_type === "MUX_Inbound__e") {
    event.set("Error_Message__c", msgTxt);
  } else {
    return null;
  }
  return org.insert({ sobject: event }, (err) => {
    if (err) {
      console.error(err);
      return null;
    } else {
      return event;
    }
  });
};

let sendMuxJobStatus = (req, res) => {
  let jobId = req.params.jobId;
  let event = nforce.createSObject("MUX_Inbound_Job_Status__e");
  event.set("MUX_Job_ID__c", jobId);
  org.insert({ sobject: event }, (err) => {
    if (err) {
      console.error(err);
      res.sendStatus(500);
    } else {
      res.sendStatus(200);
    }
  });
};

let PORT = process.env.PORT || 5000;

app.use(cors());
app.use("/", express.static(__dirname + "/www"));
app.get("/mux-jobs", getMuxJobs);
// app.get("/mux-jobs/:jobId", getJobDetails);
app.post("/send-inbound/:jobId", sendMuxInboundException);
app.post("/send-status/:jobId", sendMuxJobStatus);

let bayeux = new faye.NodeAdapter({ mount: "/faye", timeout: 45 });
bayeux.attach(server);
bayeux.on("disconnect", function (clientId) {
  console.log("Bayeux server disconnect");
});

server.listen(PORT, () => console.log(`Express server listening on ${PORT}`));

// Connect to Salesforce
let SF_CLIENT_ID = process.env.SF_CLIENT_ID;
let SF_CLIENT_SECRET = process.env.SF_CLIENT_SECRET;
let SF_USER_NAME = process.env.SF_USER_NAME;
let SF_USER_PASSWORD = process.env.SF_USER_PASSWORD;
let SF_USER_SECURITY_TOKEN = process.env.SF_USER_SECURITY_TOKEN;

let org = nforce.createConnection({
  clientId: SF_CLIENT_ID,
  clientSecret: SF_CLIENT_SECRET,
  environment: "sandbox",
  redirectUri: "http://localhost:3000/oauth/_callback",
  mode: "single",
  autoRefresh: true,
});

org.authenticate(
  {
    username: SF_USER_NAME,
    password: SF_USER_PASSWORD,
    securityToken: SF_USER_SECURITY_TOKEN,
  },
  (err) => {
    if (err) {
      console.error("Salesforce authentication error");
      console.error(err);
    } else {
      console.log("Salesforce authentication successful");
      console.log(org.oauth.instance_url);
      subscribeToPlatformEvents();
    }
  }
);

// Subscribe to Platform Events
let subscribeToPlatformEvents = () => {
  var client = new faye.Client(org.oauth.instance_url + "/cometd/40.0/");
  client.setHeader("Authorization", "OAuth " + org.oauth.access_token);
  client.subscribe("/event/MUX_Outbound__e", function (message) {
    console.log("Received MUX_Outbound__e event");
    const muxJobId = message.payload.MUX_Job_ID__c;
    const amount = message.payload.Amount__c;
    const muxJob = {
      amount,
      muxJobId,
    };
    muxJobs.push(muxJob);
    // Respond to event queue that job is pending
    createPlatformEvent(muxJobId, "MUX_Inbound_Job_Status__e", "Pending");

    // Send message to all connected Socket.io clients
    io.of("/").emit("mux_outbound", muxJob);
    delay(5000).then(() => {
      // console.log("ran after 5 second passed");
      if (amount == 13.37) {
        // In our demo, if amount is 1337 then we'll say that is a duplicate check number
        console.log("sending dupe check number exception for id", muxJobId);
        createPlatformEvent(
          muxJobId,
          "MUX_Inbound__e",
          "Duplicate Check Number"
        );
      } else if (amount == 10.01) {
        // In our demo, if amount is 10.01 then we'll say that is a duplicate payment
        console.log("sending dupe payment exception for id", muxJobId);
        createPlatformEvent(
          muxJobId,
          "MUX_Inbound__e",
          "Duplicate Payment Detected"
        );
      } else {
        createPlatformEvent(muxJobId, "MUX_Inbound_Job_Status__e", "Completed");
      }
    });
  });
};
