# This is compatibility code for older versions of sensor_msgs_py.point_cloud2
# that do not provide function read_points_numpy (e.g. ROS2 Foxy).

# The code is a slightly modified copy from:
# https://github.com/ros2/common_interfaces/blob/a1235b5c3caf8c836c0c8fa0a688dcb7c4e692a4/sensor_msgs_py/sensor_msgs_py/point_cloud2.py


from typing import Any, Iterable, List, Optional

from numpy.lib.recfunctions import structured_to_unstructured
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py.point_cloud2 import read_points


def read_points_numpy(
    cloud: PointCloud2,
    field_names: Optional[List[str]] = None,
    skip_nans: bool = False,
    uvs: Optional[Iterable] = None,
    reshape_organized_cloud: bool = False,
) -> Any:
    """Read equally typed fields from sensor_msgs.PointCloud2 message as a unstructured numpy array.

    This method is better suited if one wants to perform math operations
    on e.g. all x,y,z fields.
    But it is limited to fields with the same dtype as unstructured numpy arrays
    only contain one dtype.

    :param cloud: The point cloud to read from sensor_msgs.PointCloud2.
    :param field_names: The names of fields to read. If None, read all fields.
                        (Type: Iterable, Default: None)
    :param skip_nans: If True, then don't return any point with a NaN value.
                      (Type: Bool, Default: False)
    :param uvs: If specified, then only return the points at the given
        coordinates. (Type: Iterable, Default: None)
    :param reshape_organized_cloud: Returns the array as an 2D organized point cloud if set.

    :return: Numpy array containing all points.
    """
    assert all(
        cloud.fields[0].datatype == field.datatype
        for field in cloud.fields[1:]
        if field_names is None or field.name in field_names
    ), "All fields need to have the same datatype. Use `read_points()` otherwise."
    structured_numpy_array = read_points(cloud, field_names, skip_nans, uvs, reshape_organized_cloud)
    return structured_to_unstructured(structured_numpy_array)
