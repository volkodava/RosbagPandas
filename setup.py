from distutils.core import setup

setup(name='rosbag_core',
      version='0.4.0.0',
      author='Adam Taylor',
      author_email='aktaylor08@gmail.com',
      description='Create a Python pandas data frame from a ros bag file',
      py_modules=['rosbag_core'],
      scripts=['bag2json.py'],
      keywords=['ROS', 'rosbag', 'json'],
      url='https://github.com/aktaylor08/RosbagPandas',
      )
