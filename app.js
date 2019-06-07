const express = require("express");
const csv = require("csvtojson");

const app = express();

const metaData = require("./assets/metaData.json");
let users = [];

app.get("/report", async (req, res) => {
  const singleSourceOfTruth = await consolidateData();
  users.map(data => {
    data["revenue"] =
      data.origin === "app_store"
        ? Number(data.fee) * 0.7
        : Number(data.fee) * 0.9;
  });
  const totalRevenue = Math.round(
    users.reduce((acc, item) => acc + (Number(item.revenue) || 0), 0)
  );
  const totalSecondsStreamed = singleSourceOfTruth.reduce(
    (acc, item) => acc + (Number(item.seconds) || 0),
    0
  );
  const response = Object.values(
    singleSourceOfTruth.reduce((c, { label, seconds }) => {
      c[label] = c[label] || { label, seconds: 0 };
      c[label].seconds += Number(seconds);
      return c;
    }, {})
  );

  response.map(data => {
    data["percentage_streamed"] = Math.round(
      (data.seconds / totalSecondsStreamed) * 100
    );
    data["revenue_split"] = (totalRevenue / 100) * data["percentage_streamed"];
  });
  const body = response.reduce(
    (obj, item) => ((obj[item.label] = `${item.revenue_split}`), obj),
    {}
  );
  res.send(body);
});

app.get("/users/:user_id", async (req, res) => {
  const requestedUserInfo = req.params.user_id;
  const singleSourceOfTruth = await consolidateData();
  const streamedCollectionForRequestedUser = singleSourceOfTruth.filter(
    data => data.user_id === requestedUserInfo
  );
  if (streamedCollectionForRequestedUser.length > 0) {
    const totalSecondsStreamedForRequestedUser = streamedCollectionForRequestedUser.reduce(
      (acc, item) => acc + (Number(item.seconds) || 0),
      0
    );
    res.send({
      User: req.params.user_id,
      "Total Streamed": totalSecondsStreamedForRequestedUser
    });
  } else {
    res.status(204).end();
  }
});

async function csvToJson(filePath) {
  return await csv().fromFile(filePath);
}

async function consolidateData() {
  const streamingData = await csvToJson("./assets/streaming.csv");
  const userData = await csvToJson("./assets/users.csv");
  users = userData;
  streamingData.map(data => {
    data = Object.assign(
      data,
      userData.filter(user => user.user_id === data.user_id)[0]
    );
    return Object.assign(
      data,
      metaData.filter(meta => meta.track_id === data.track_id)[0]
    );
  });
  return streamingData;
}

const hostname = "127.0.0.1";
const port = 3000;
app.listen(port, () => {
  console.log(`Serving running at http://${hostname}:${port}/`);
});
