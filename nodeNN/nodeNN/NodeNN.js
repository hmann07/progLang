
var Network = function() {
  this.layers = [];
  this.connections;
  this.inputLayer;
  this.outputLayer;
  this.learningRate = 0.3;
  this.globalActivationFn = ActivationFunctions.sigmoid;
}


// take in an array of ints, length of array is number of layers, value of each item indicates nodes in the array e.g. [2,3,3,1]

Network.prototype.build = function(layerspec){
  console.log("building layers...")
  nn = this
  layerspec.forEach(function(l,i){
    console.log(" adding layer...")
    var nl = new NeuronLayer(l,i,nn)
    nn.layers.push(nl)

  })


}

// Connect up all the neurons

Network.prototype.connect = function(){
  console.log("connecting layers...")
  nn = this
  nn.layers.forEach(function(layer){
    layer.connect()
  })
}

// for all layers print out its contents.

Network.prototype.describe = function(){
    this.layers.forEach(function(layer){console.log(layer)});
}

// function to set all weights on connections, biases on the nodes.

Network.prototype.initialise = function(){
    this.layers.forEach(function(layer){
      layer.neurons.forEach(function(neuron){
        neuron.bias = -1 //Math.random() * (1 - -1) + -1;
        neuron.outgoingConnections.forEach(function(connection){
          connection.weight = Math.random() * (2.5 - -2.5) + -2.5;
        })
      })

    })
}

// run one pattern through the network

Network.prototype.activate = function(pattern){
  //console.log(this.layers.length)
  console.log(pattern)
  this.layers.forEach(function(currentLayer){
    //console.log(currentLayer.layerNumber)
    var thislayerNumber = currentLayer.layerNumber
    currentLayer.neurons.forEach(function(currentNeuron,inputneuron){
      if(thislayerNumber==0){
        currentNeuron.output = pattern.inputs[inputneuron]
        currentNeuron.outgoingConnections.forEach(function(conn){
          conn.signal = pattern.inputs[inputneuron]
        })
      } else{
        currentNeuron.output = currentNeuron.activationFunction(currentNeuron.linearCombine()-currentNeuron.bias)
        //console.log(currentNeuron.output)
        currentNeuron.outgoingConnections.forEach(function(conn){
          conn.signal = currentNeuron.output
        })
      }
    })
  })
  this.error(pattern.expectedOutputs) //calcualte error at output nodes
}


// back propagastion

Network.prototype.learn = function(){
  var nn = this.layers
  var lastLayer = nn.length-1

  nn[lastLayer].neurons.forEach(function(neuron){
    neuron.updateOutputWeights();
  })

  for (var i = nn.length-2; i >= 0; i--){
    nn[i].neurons.forEach(function(neuron){
      neuron.updateHiddenWeights();
    })
  }

}

//error, should take an a erray of expected outputs that should be compared with the actual netwrok outputs
Network.prototype.error = function(y){

  var nn = this.layers
  var lastLayer = nn.length-1
  //console.log(lastLayer)
  var outputerror = y.forEach(function(y1,i){
    nn[lastLayer].neurons[i].error = y1 - nn[lastLayer].neurons[i].output
    console.log("Actual:" + nn[lastLayer].neurons[i].output + ",Error: " + nn[lastLayer].neurons[i].error)
  })
}


Network.prototype.getError = function(){

  var nn = this.layers
  var lastLayer = nn.length-1
  //console.log(lastLayer)
  var t = 0
  nn[lastLayer].neurons.forEach(function(n,acc){
    t += n.error
  })
  return t
  }


// A single nerual computing unit. Will have

var Neuron = function(nn){
  this.network = nn
  this.activationFunction = nn.globalActivationFn
  this.bias = 0;
  this.outgoingConnections = [];
  this.incomingConnections = [];
  this.output = 0;
  this.error = 0;
  this.errorGradient = 0;
  this.learningRate = nn.learningRate;

}

// the combiner inside the neuron. will take the sum of weight * signal for all incoming connections to the Neuron.

Neuron.prototype.linearCombine = function(){
   //var total = this.incomingConnections.reduce((current, acc) => acc + (current.weight * current.signal));
   var total = 0
   this.incomingConnections.forEach(function(conn){
     total = total + (conn.weight * conn.signal)
   })
   return total;
}

Neuron.prototype.outputLayerErrorGradient = function(input,err){
 return  input * (1-input) * err
}

Neuron.prototype.hiddenLayerErrorGradient = function(){
  var n = this
    //console.log(n.outgoingConnections)
    var sumErrorGradient = 0
    n.outgoingConnections.forEach(function(conn){
    sumErrorGradient = sumErrorGradient + (conn.outputNode.errorGradient * conn.oldWeight)
  })
 return  n.output * (1-n.output) * sumErrorGradient
}

Neuron.prototype.updateHiddenWeights = function(){
        var n = this
        n.errorGradient = n.hiddenLayerErrorGradient()
        n.bias = n.learningRate * n.bias * n.errorGradient
        n.incomingConnections.forEach(function(conn){
          conn.oldWeight = conn.weight
          conn.weight = conn.weight + (n.learningRate * conn.signal * n.errorGradient)
        })
  }


Neuron.prototype.updateOutputWeights = function(){
        var n = this
        n.errorGradient = n.outputLayerErrorGradient(n.output,n.error)
        n.bias = n.learningRate * n.bias * n.errorGradient
        n.incomingConnections.forEach(function(conn){
          //console.log("setting weight")
          conn.oldWeight = conn.weight
          conn.weight = conn.weight + (n.learningRate * conn.signal * n.errorGradient)
        })

}



var NeuronConnection = function(froml,tol,fromn,ton){
  this.fromlayer = froml;
  this.tolayer = tol;
  this.inputNode = fromn;
  this.outputNode = ton;
  this.weight = 0;
  this.oldWeight = 0;
  this.signal = 0;
}

NeuronConnection.prototype.updateWeight = function(d){
  this.weight = d(this.weight);
}


var InputLayer = function(){
  this.inputs = [];
}

var outputLayer = function(){
  this.inputs = [];
}

// the NeuronLayer will take an Int as an argument and create that number of neurons

var NeuronLayer = function(numberofnodes,layernumber,network){
  this.network = network
  this.numberofnodes = numberofnodes
  this.layerNumber = layernumber;
  this.neurons = [];

  for(var a = 0;a<numberofnodes;a++){
    var n = new Neuron(this.network)
    this.neurons.push(n)
    console.log("   adding " + a + " neuron to layer " + layernumber)
  }
}

// for each neuron in the layer, create a connection against all neurons in previous layer.

NeuronLayer.prototype.connect = function(){
  var nn = this.network
  var ln = this.layerNumber
  var nextlayer = (ln==(nn.layers.length-1)?ln:(ln + 1))
  this.neurons.forEach(function(sourceneuron){
      nn.layers[nextlayer].neurons.forEach(function(targetneuron){
        console.log("   connecting layer")

        var conn = new NeuronConnection(ln,nextlayer,sourceneuron,targetneuron)
        targetneuron.incomingConnections.push(conn)
        sourceneuron.outgoingConnections.push(conn)

    })
  })
}


var ActivationFunctions = {
  sigmoid: function(value){ return(1 / ( 1 + Math.pow(Math.E,-1*value)))},
  linear: function(value){return value},
  step: function(value){return (value<0?0:1)},
  sign: function(value){return (value<0?-1:1)},
  tanh: function(value){return value}
}

var testData = [
    {inputs: [1,1], expectedOutputs: [0]},
    {inputs: [0,1], expectedOutputs: [1]},
    {inputs: [1,0], expectedOutputs: [1]},
    {inputs: [0,0], expectedOutputs: [0]}
  ]

var n = new Network();
n.build([2,10,10,1])
n.connect()
n.initialise()


var epoch = [];
var sumsquarederror = function(){
  var r = 0
   epoch.forEach(function(e){

    r += (e*e)
  })
  return r
}

var converged = 100000000000000
var iter = 0
for (iter ; iter <1000000 && (converged > 0.001) ;iter++){
  var pattern = iter % testData.length
  n.activate(testData[pattern])
  epoch[pattern] = n.getError()
  console.log(epoch)
  converged = sumsquarederror()
  n.learn()

}
console.log(iter + " iterations")
