const processMessages = async (message) => {
  try {
    console.log({ message });
  } catch (error) {
    console.log(error);
  }
};

export default processMessages;
