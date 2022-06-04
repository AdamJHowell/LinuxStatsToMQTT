# This tool will connect to a MQTT broker, and publish simple MQTT messages containing system stats.
# I primarily use this to track CPU temperature in Node-RED.
# The sole command-line-parameter is the configuration file.
# The configuration file is in JSON format.
# It must contain "brokerAddress", "brokerPort", "brokerQoS", "publishTopic", "controlTopic", and "publishInterval".
# https://pypi.org/project/paho-mqtt/
# This requires Python version 3.10 or higher.

import sys
import json
import time
import socket
import datetime
import gpiozero as gz
import paho.mqtt.client as mqtt
from uuid import getnode as get_mac

telemetry = json.loads( '{}' )
configuration = json.loads( '{}' )
config_file_name = "config.json"
last_publish = 0
telemetry['macAddress'] = ':'.join( ("%012X" % get_mac())[i:i + 2] for i in range( 0, 12, 2 ) )
client = mqtt.Client( client_id = telemetry['macAddress'] )


def on_connect( con_client, userdata, flags, result ):
  if result != 0:
    print( "Bad connection, returned code: ", result )
  if result == 2112:  # This should be unreachable, and should not cause problems if it is reached.
    print( str( con_client ) )
    print( str( userdata ) )
    print( str( flags ) )


def on_disconnect( dc_client, userdata, result ):
  print( "Disconnected from the broker!  Reason: " + str( result ) )
  if result == 2112:  # This should be unreachable, and should not cause problems if it is reached.
    print( str( dc_client ) )
    print( str( userdata ) )


def on_message( sub_client, userdata, msg ):
  global configuration
  global last_publish
  message = json.loads( str( msg.payload.decode( 'utf-8' ) ) )
  print( json.dumps( message, indent = '\t' ) )
  if 'command' in message:
    command = message['command']
    print( "Processing command \"" + command + "\"" )
    if command == "publishTelemetry":
      poll_telemetry()
      publish_telemetry()
      last_publish = epoch_time()
    elif command == "changeTelemetryInterval":
      old_value = configuration['publishInterval']
      new_value = message['value']
      if old_value != new_value and new_value > 4:
        print( "Old publish interval: " + old_value )
        configuration['publishInterval'] = new_value
        print( "New publish interval: " + configuration['publishInterval'] )
      else:
        print( "Not changing the telemetry publish interval." )
    elif command == "publishStatus":
      publish_telemetry()
    elif command == "debug":
      print( str( sub_client ) )
      print( str( userdata ) )
    else:
      print( "The command \"" + str( command ) + "\" is not recognized." )
      print( "Currently recognized commands are:\n\tpublishTelemetry\n\tchangeTelemetryInterval\n\tchangeSeaLevelPressure\n\tpublishStatus" )
  else:
    print( "Message did not contain a command property." )


def on_publish( pub_client, userdata, result ):
  if result == 2112.2112:  # This should be unreachable, and should not cause problems if it is reached.
    print( str( pub_client ) )
    print( str( userdata ) )
    print( str( result ) )


def get_ip():
  sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
  try:
    sock.connect( ("8.8.8.8", 80) )
    ip = sock.getsockname()[0]
  except InterruptedError:
    ip = '127.0.0.1'
  except OSError:
    ip = '127.0.0.1'
  finally:
    sock.close()
  return ip


def epoch_time():
  # Returns the time in seconds since Unix Epoch, rounded to an integer.
  return round( time.time() )


def get_timestamp():
  # Returns the current date and time in ISO-8601 format.
  return datetime.datetime.now().strftime( "%Y-%m-%d %H:%M:%S" )


def publish_telemetry():
  telemetry['timeStamp'] = get_timestamp()
  client.publish( topic = configuration['publishTopic'], payload = json.dumps( telemetry, indent = '\t' ), qos = configuration['brokerQoS'] )
  print( "Publishing this to '" + configuration['publishTopic'] + "':" )
  print( json.dumps( telemetry, indent = 3 ) )


def poll_telemetry():
  telemetry['cpuTemp'] = gz.CPUTemperature().temperature


def close_mqtt():
  client.unsubscribe( configuration['controlTopic'] )
  client.loop_stop()
  client.disconnect()


def main( argv ):
  global configuration
  global config_file_name
  global last_publish

  try:
    if len( argv ) > 1:
      config_file_name = argv[1]
    print( "Using " + config_file_name + " as the config file." )
    # Read in the configuration file.
    with open( config_file_name, "r" ) as config_file:
      configuration = json.load( config_file )

    # Create the Dictionary to hold results, and set the static components.
    telemetry['ipAddress'] = get_ip()
    host_name = socket.gethostname()
    telemetry['host'] = host_name
    telemetry['timeStamp'] = get_timestamp()
    if 'notes' in configuration:
      telemetry['notes'] = configuration['notes']
    telemetry['brokerAddress'] = configuration['brokerAddress']
    telemetry['brokerPort'] = configuration['brokerPort']
    print( "Hostname: " + host_name )
    print( "IP address: " + telemetry['ipAddress'] )
    print( "Current time: " + get_timestamp() )
    print( "Using broker address: " + configuration['brokerAddress'] )
    print( "Using broker port: " + configuration['brokerPort'] )
    print( "Publishing to the telemetry topic: \"" + configuration['publishTopic'] + "\"" )
    print( "Subscribing to the control topic: \"" + configuration['controlTopic'] + "\"" )
    print( "Publishing and subscribing using QoS: " + str( configuration['brokerQoS'] ) )
    print( "Waiting " + str( configuration['publishInterval'] ) + " seconds between publishes (non-blocking)." )

    # Assign callback functions.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_message = on_message
    client.on_disconnect = on_disconnect  # This is throwing: "TypeError: on_disconnect() takes 0 positional arguments but 3 were given" when the program closes.

    # Connect using the details from the configuration file.
    client.connect( configuration['brokerAddress'], int( configuration['brokerPort'] ) )
    # Subscribe to the control topic.
    result_tuple = client.subscribe( configuration['controlTopic'], configuration['brokerQoS'] )
    if result_tuple[0] == 0:
      print( "Successfully subscribed to the control topic: \"" + configuration['controlTopic'] + "\"" )

    client.loop_start()
    while True:
      if not client.is_connected():
        try:
          # Don't flood the broker with reconnect attempts.
          time.sleep( 3 )
          client.reconnect()
        except TimeoutError:
          print( "Timeout encountered while trying to reconnect to the MQTT broker!" )
          close_mqtt()
          break
      current_time = epoch_time()
      interval = configuration['publishInterval']
      if current_time - interval > last_publish:
        poll_telemetry()
        publish_telemetry()
        last_publish = epoch_time()

  except KeyboardInterrupt:
    print( "\nKeyboard interrupt detected, exiting...\n" )
    close_mqtt()
  except KeyError as key_error:
    log_string = "Python dictionary key error: %s" % str( key_error )
    print( log_string )
  except ConnectionRefusedError as connection_error:
    print( "Connection error: " + str( connection_error ) )
  except TimeoutError:
    print( "Timeout encountered while connecting to the MQTT broker!" )
    close_mqtt()


if __name__ == "__main__":
  main( sys.argv )
