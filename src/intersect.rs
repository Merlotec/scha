use nalgebra::Vector2;
use rayon::prelude::*;
use std::f64::consts::PI;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use crate::assign::Circle;

/// A hashable key for points, based on their bit representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PointKey {
    x_bits: u64,
    y_bits: u64,
}

impl From<Vector2<f64>> for PointKey {
    fn from(v: Vector2<f64>) -> Self {
        PointKey {
            x_bits: v.x.to_bits(),
            y_bits: v.y.to_bits(),
        }
    }
}

impl Hash for PointKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.x_bits.hash(state);
        self.y_bits.hash(state);
    }
}


pub fn overlap(circle: Circle, others: &[Circle], samples: usize) -> f64 {
    // If the circle has zero (or negative) radius, no area
    if circle.r <= 0.0 {
        return 0.0;
    }
    if others.is_empty() {
        return 0.0;
    }

    let (cx, cy) = (circle.origin.x, circle.origin.y);
    let r = circle.r;
    let min_x = cx - r;
    let max_x = cx + r;
    let min_y = cy - r;
    let max_y = cy + r;

    if min_x >= max_x || min_y >= max_y {
        return 0.0;
    }

    // Increase resolution for better accuracy
    let samples_per_dimension = samples;
    let dx = (max_x - min_x) / (samples_per_dimension as f64);
    let dy = (max_y - min_y) / (samples_per_dimension as f64);
    let cell_area = dx * dy;

    // We'll create an iterator over all sample points using indices:
    let total_samples = samples_per_dimension * samples_per_dimension;

    // Collect the sum of areas in parallel:
    let sum_area = (0..total_samples).into_par_iter().map(|idx| {
        // Convert idx -> (i, j)
        let i = idx / samples_per_dimension;
        let j = idx % samples_per_dimension;

        let x = min_x + (i as f64 + 0.5) * dx;
        let y = min_y + (j as f64 + 0.5) * dy;

        // Check if inside main circle
        let dist_sq_main = (x - cx) * (x - cx) + (y - cy) * (y - cy);
        if dist_sq_main <= r * r {
            // Check if inside at least one other circle
            for c in others {
                let dx_o = x - c.origin.x;
                let dy_o = y - c.origin.y;
                if dx_o*dx_o + dy_o*dy_o <= c.r * c.r {
                    return cell_area; // This sample is in overlap
                }
            }
        }
        0.0
    }).sum::<f64>();

    sum_area
}

pub fn intersect_all_approx(circles: &[Circle]) -> f64 {
    // Handle trivial cases
    if circles.is_empty() {
        return 0.0;
    }
    if circles.len() == 1 {
        let r = circles[0].r;
        return std::f64::consts::PI * r * r;
    }

    // Compute intersection of bounding boxes
    // Each circle's bounding box is (cx - r, cy - r) to (cx + r, cy + r)
    let mut min_x = f64::NEG_INFINITY;
    let mut min_y = f64::NEG_INFINITY;
    let mut max_x = f64::INFINITY;
    let mut max_y = f64::INFINITY;

    for c in circles {
        let (cx, cy) = (c.origin.x, c.origin.y);
        let r = c.r;

        // Update intersection bounding box
        // Intersection of boxes:
        //   new_min_x = max(old_min_x, cx - r)
        //   new_max_x = min(old_max_x, cx + r)
        //   new_min_y = max(old_min_y, cy - r)
        //   new_max_y = min(old_max_y, cy + r)

        min_x = min_x.max(cx - r);
        min_y = min_y.max(cy - r);
        max_x = max_x.min(cx + r);
        max_y = max_y.min(cy + r);

        // If at any point no intersection remains, return 0
        if min_x > max_x || min_y > max_y {
            return 0.0;
        }
    }

    // If the bounding box is zero or negative in dimension, area is zero
    if min_x >= max_x || min_y >= max_y {
        return 0.0;
    }

    // Approximate using sampling
    // Increase these values for higher accuracy but slower computation
    let samples_per_dimension = 500;
    let dx = (max_x - min_x) / (samples_per_dimension as f64);
    let dy = (max_y - min_y) / (samples_per_dimension as f64);

    let mut inside_count = 0;
    let total_samples = samples_per_dimension * samples_per_dimension;

    for i in 0..samples_per_dimension {
        for j in 0..samples_per_dimension {
            let x = min_x + (i as f64 + 0.5) * dx; // center of each cell
            let y = min_y + (j as f64 + 0.5) * dy;

            // Check if (x, y) is inside all circles
            let mut inside_all = true;
            for c in circles {
                let (cx, cy) = (c.origin.x, c.origin.y);
                let r = c.r;
                let dist_sq = (x - cx) * (x - cx) + (y - cy) * (y - cy);
                if dist_sq > r * r {
                    inside_all = false;
                    break;
                }
            }

            if inside_all {
                inside_count += 1;
            }
        }
    }

    let bounding_box_area = (max_x - min_x) * (max_y - min_y);
    let fraction = inside_count as f64 / total_samples as f64;
    fraction * bounding_box_area
}

pub fn intersect_all(circles: &[Circle]) -> f64 {
    match circles.len() {
        0 => 0.0,
        1 => PI * circles[0].r * circles[0].r,
        _ => intersection_of_many_circles(circles),
    }
}

/// Computes the intersection area of multiple circles using geometric decomposition.
/// Steps:
/// 1. Find pairwise intersection points of all circles.
/// 2. Keep only those intersection points that lie inside all circles.
/// 3. On each circle, sort intersection points and determine which arcs form the intersection boundary.
/// 4. Compute the area of the resulting shape from arcs and polygonal areas.
fn intersection_of_many_circles(circles: &[Circle]) -> f64 {
    if !has_common_intersection(circles) {
        return 0.0;
    }

    // Get all pairwise intersection points
    let mut points = Vec::new();
    let n = circles.len();
    for i in 0..n {
        for j in (i+1)..n {
            let pts = circle_circle_intersection(&circles[i], &circles[j]);
            for &p in &pts {
                // Check if p is inside all circles
                if inside_all(p, circles) {
                    points.push(p);
                }
            }
        }
    }

    if points.is_empty() {
        // Check if there's a circle fully inside all others
        let mut min_area: Option<f64> = None;
        'outer: for c in circles {
            if inside_all(c.origin, circles) {
                for c2 in circles {
                    if (c2.r + 1e-14) < distance(c.origin, c2.origin) + c.r {
                        continue 'outer; // not fully inside
                    }
                }
                let a = PI * c.r * c.r;
                min_area = Some(match min_area {
                    Some(ma) => ma.min(a),
                    None => a
                });
            }
        }
        return min_area.unwrap_or(0.0);
    }

    let mut boundary_arcs = Vec::new();

    // Group intersection points by circle:
    let mut circle_points = vec![Vec::new(); n];
    for &p in &points {
        for (i, c) in circles.iter().enumerate() {
            let d = distance(p, c.origin);
            if (d - c.r).abs() < 1e-12 {
                circle_points[i].push(p);
            }
        }
    }

    for (ci, c) in circles.iter().enumerate() {
        let center = c.origin;
        let mut pts = circle_points[ci].clone();
        if pts.is_empty() {
            continue;
        }
        pts.sort_by(|&a, &b| {
            let ang_a = (a - center).y.atan2((a - center).x);
            let ang_b = (b - center).y.atan2((b - center).x);
            ang_a.partial_cmp(&ang_b).unwrap()
        });

        for i in 0..pts.len() {
            let p1 = pts[i];
            let p2 = pts[(i+1) % pts.len()];
            let arc_mid_angle = mid_angle_for_circle_arc(c, p1, p2);
            let arc_mid_point = center + Vector2::new(c.r * arc_mid_angle.cos(), c.r * arc_mid_angle.sin());
            if inside_all(arc_mid_point, circles) {
                boundary_arcs.push((ci, p1, p2));
            }
        }
    }

    if boundary_arcs.is_empty() {
        return 0.0;
    }

    // Build adjacency
    let mut adjacency: HashMap<PointKey, Vec<(PointKey, usize)>> = HashMap::new();
    let mut unique_points_map: HashMap<PointKey, Vector2<f64>> = HashMap::new();
    for (ci, p1, p2) in &boundary_arcs {
        let p1_key = PointKey::from(*p1);
        let p2_key = PointKey::from(*p2);

        adjacency.entry(p1_key).or_default().push((p2_key, *ci));
        adjacency.entry(p2_key).or_default().push((p1_key, *ci));
        unique_points_map.insert(p1_key, *p1);
        unique_points_map.insert(p2_key, *p2);
    }

    let start = *unique_points_map.values().next().unwrap();
    let polygon_points = build_boundary_polygon(&adjacency, &unique_points_map, start);

    let polygon_area = polygon_area(&polygon_points);

    let mut arc_area_sum = 0.0;
    for i in 0..polygon_points.len() {
        let p1 = polygon_points[i];
        let p2 = polygon_points[(i+1)%polygon_points.len()];

        // find arc circle
        let p1_key = PointKey::from(p1);
        let p2_key = PointKey::from(p2);
        let circles_for_edge = &adjacency[&p1_key];
        let mut arc_circle = None;
        for (candidate, ci) in circles_for_edge {
            if *candidate == p2_key {
                arc_circle = Some(*ci);
                break;
            }
        }
        let ci = arc_circle.expect("No circle found for arc - invalid geometry");
        let c = circles[ci];
        arc_area_sum += arc_segment_area(c, p1, p2);
    }

    polygon_area + arc_area_sum
}

/// Check if all circles overlap in some region quickly by comparing bounding boxes
fn has_common_intersection(circles: &[Circle]) -> bool {
    let mut min_x = f64::NEG_INFINITY;
    let mut min_y = f64::NEG_INFINITY;
    let mut max_x = f64::INFINITY;
    let mut max_y = f64::INFINITY;

    for c in circles {
        let x0 = c.origin.x - c.r;
        let x1 = c.origin.x + c.r;
        let y0 = c.origin.y - c.r;
        let y1 = c.origin.y + c.r;

        min_x = min_x.max(x0);
        min_y = min_y.max(y0);
        max_x = max_x.min(x1);
        max_y = max_y.min(y1);

        if min_x > max_x || min_y > max_y {
            return false;
        }
    }
    true
}

/// Return intersection points of two circles (0,1, or 2 points)
fn circle_circle_intersection(c1: &Circle, c2: &Circle) -> Vec<Vector2<f64>> {
    let d = distance(c1.origin, c2.origin);
    if d > c1.r + c2.r + 1e-14 {
        return vec![];
    }
    if d < (c1.r - c2.r).abs() - 1e-14 {
        return vec![];
    }

    if d < 1e-14 && (c1.r - c2.r).abs() < 1e-14 {
        return vec![];
    }

    let a = (c1.r*c1.r - c2.r*c2.r + d*d) / (2.0*d);
    let h = (c1.r*c1.r - a*a).sqrt();

    let p0 = c1.origin;
    let p1 = c2.origin;

    let mid = p0 + (p1 - p0)*(a/d);
    if h.abs() < 1e-14 {
        return vec![mid];
    }

    let rx = -(p1.y - p0.y)*(h/d);
    let ry =  (p1.x - p0.x)*(h/d);

    let i1 = Vector2::new(mid.x + rx, mid.y + ry);
    let i2 = Vector2::new(mid.x - rx, mid.y - ry);
    vec![i1, i2]
}

/// Check if point p is inside all circles
fn inside_all(p: Vector2<f64>, circles: &[Circle]) -> bool {
    for c in circles {
        if distance(p, c.origin) > c.r + 1e-14 {
            return false;
        }
    }
    true
}

/// Euclidean distance
fn distance(a: Vector2<f64>, b: Vector2<f64>) -> f64 {
    (a - b).norm()
}

/// Compute the mid-angle of the arc on circle c going from p1 to p2 counterclockwise.
fn mid_angle_for_circle_arc(c: &Circle, p1: Vector2<f64>, p2: Vector2<f64>) -> f64 {
    let v1 = p1 - c.origin;
    let v2 = p2 - c.origin;
    let ang1 = v1.y.atan2(v1.x);
    let ang2 = v2.y.atan2(v2.x);
    let mut dtheta = ang2 - ang1;
    if dtheta < 0.0 { dtheta += 2.0*PI; }
    ang1 + dtheta/2.0
}

/// Build boundary polygon from adjacency map by following arcs in order.
/// We assume it forms a single closed loop.
fn build_boundary_polygon(
    adjacency: &std::collections::HashMap<PointKey, Vec<(PointKey, usize)>>,
    unique_points_map: &std::collections::HashMap<PointKey, Vector2<f64>>,
    start: Vector2<f64>
) -> Vec<Vector2<f64>> {
    let mut polygon = Vec::new();
    polygon.push(start);
    let mut current = start;
    let mut prev = PointKey { x_bits: u64::MAX, y_bits: u64::MAX };

    loop {
        let current_key = PointKey::from(current);
        let neighbors = &adjacency[&current_key];

        let maybe_next_key = if polygon.len() == 1 {
            // Just take the first neighbor
            Some(neighbors[0].0)
        } else {
            // Filter out the previous point to move forward
            let pprev = prev;
            neighbors.iter()
                .map(|x| x.0)
                .filter(|&x| x != pprev)
                .next()
        };

        let next_key = match maybe_next_key {
            Some(nk) => nk,
            None => {
                // Handle the case when no next key is found.
                // This might indicate a degenerate polygon or unexpected geometry.
                // Decide what to do: return the polygon as is, or return an empty vector, etc.
                // For now, let's break out, returning the polygon as constructed so far.
                break;
            }
        };

        let next_v = unique_points_map[&next_key];
        if (next_v - start).norm() < 1e-14 && polygon.len() > 1 {
            // closed loop found
            break;
        }
        polygon.push(next_v);
        prev = PointKey::from(current);
        current = next_v;
    }

    polygon
}

/// Compute polygon area using shoelace formula
fn polygon_area(points: &[Vector2<f64>]) -> f64 {
    let n = points.len();
    let mut area = 0.0;
    for i in 0..n {
        let j = (i+1)%n;
        area += points[i].x * points[j].y - points[j].x * points[i].y;
    }
    area.abs()*0.5
}

/// Compute the arc segment area for arc on circle c from p1 to p2.
fn arc_segment_area(c: Circle, p1: Vector2<f64>, p2: Vector2<f64>) -> f64 {
    let v1 = p1 - c.origin;
    let v2 = p2 - c.origin;
    let ang1 = v1.y.atan2(v1.x);
    let ang2 = v2.y.atan2(v2.x);
    let mut dtheta = ang2 - ang1;
    if dtheta < 0.0 { dtheta += 2.0*PI; }

    if dtheta > PI {
        dtheta = 2.0*PI - dtheta;
    }

    let r = c.r;
    let segment_area = (r*r/2.0)*(dtheta - dtheta.sin());

    segment_area
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_no_circles() {
        let circles = [];
        assert_eq!(intersect_all(&circles), 0.0);
    }

    #[test]
    fn test_one_circle() {
        let c = Circle { origin: Vector2::new(0.0,0.0), r:1.0 };
        let area = intersect_all(&[c]);
        let expected = PI * 1.0 * 1.0;
        assert!((area - expected).abs() < 1e-12);
    }

    #[test]
    fn test_two_separate_circles() {
        let c1 = Circle { origin: Vector2::new(0.0,0.0), r:1.0 };
        let c2 = Circle { origin: Vector2::new(3.0,0.0), r:1.0 };
        let area = intersect_all(&[c1,c2]);
        assert!(area.abs() < 1e-12); // no intersection
    }

    #[test]
    fn test_two_intersecting_circles() {
        let c1 = Circle { origin: Vector2::new(0.0,0.0), r:1.0 };
        let c2 = Circle { origin: Vector2::new(0.5,0.0), r:1.0 };
        let area = intersect_all(&[c1,c2]);
        // Known intersection area of two unit circles offset by 0.5: ~2.228369...
        println!("xix:@ {}", area);
        assert!((area - 2.1521).abs() < 0.05);
    }
}
